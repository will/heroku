require 'uri'
class PgDumpRestore
  attr_reader :command

  def initialize(source, target, command)
    @source = URI.parse(source)
    @target = URI.parse(target)
    @command = command

    fill_in_shorthand_uris!
  end

  def execute
    prepare
    run
    verify
  end

  def prepare
    if @target.host == 'localhost'
      create_local_db
    else
      ensure_remote_db_empty
    end
  end

  def verify
    verify_extensions_match
  end

  def dump_restore_cmd
    dump_env, dump_cmd = gen_pg_restore_command(@target)
    restore_env, restore_cmd = gen_pg_dump_command(@source)
    r, w = IO.pipe
    dump_pid = Process.spawn(dump_env, dump_cmd, :out=>w)
    restore_pid = Process.spawn(restore_env, restore_cmd, :in=>r)
   # 2.times do
   #   wait_pid, wait_status = Process.wait2
   #   raise "Unexpected child process terminated" unless [dump_pid, restore_pid].include? wait_pid
   #   raise "Child process terminated unsuccessfully" unless wait_status.success?
   # end

    Process.waitpid(dump_pid)
    w.close
    Process.waitpid(restore_pid)
  end

  private

  def create_local_db
    dbname = @target.path[1..-1]
    cdb_output = `createdb #{dbname} 2>&1`
    if $?.exitstatus != 0
      if cdb_output =~ /already exists/
        command.error(cdb_output + "\nPlease drop the local database (`dropdb #{dbname}`) and try again.")
      else
        command.error(cdb_output + "\nUnable to create new local database. Ensure your local Postgres is working and try again.")
      end
    end
  end

  def ensure_remote_db_empty
    sql = 'select count(*) = 0 from pg_stat_user_tables;'
    result = exec_sql_on_uri(sql, @target)
    unless result == " ?column? \n----------\n t\n(1 row)\n\n"
      command.error("Remote database is not empty.\nPlease create a new database, or use `heroku pg:reset`")
    end
  end

  def gen_pg_dump_command(uri)
    # It is occasionally necessary to override PGSSLMODE, as when the server
    # wasn't built to support SSL.
    [ {'PGPASSWORD'=>uri.password, 'PGSSLMODE'=>'prefer'},
      %Q(pg_dump --verbose -F c -Z 0 #{connstring(uri, :skip_d_flag)})
    ]
  end

  def gen_pg_restore_command(uri)
    [ {'PGPASSWORD'=>uri.password},
      %Q(pg_restore --verbose --no-acl --no-owner #{connstring(uri)})
    ]
  end

  def connstring(uri, skip_d_flag=false)
    database = uri.path[1..-1]
    user = uri.user ? "-U #{uri.user}" : ""
    %Q{#{user} -h #{uri.host} -p #{uri.port} #{skip_d_flag ? '' : '-d'} #{database} }
  end

  def fill_in_shorthand_uris!
    [@target, @source].each do |uri|
      uri.host ||= 'localhost'
      uri.port ||= Integer(ENV['PGPORT'] || 5432)
    end
  end

  def verify_extensions_match
    # It's pretty common for local DBs to not have extensions available that
    # are used by the remote app, so take the final precaution of warning if
    # the extensions available in the local database don't match. We don't
    # report it if the difference is solely in the version of an extension
    # used, though.
    ext_sql = "SELECT extname FROM pg_extension ORDER BY extname;"
    target_exts = exec_sql_on_uri(ext_sql, @target)
    source_exts = exec_sql_on_uri(ext_sql, @source)
    if target_exts != source_exts
      command.error <<-EOM
WARNING: Extensions in newly created target database differ from existing source database.

Target extensions:
#{target_exts}
Source extensions:
#{source_exts}
HINT: You should review output to ensure that any errors
ignored are acceptable - entire tables may have been missed, where a dependency
could not be resolved. You may need to to install a postgresql-contrib package
and retry.
EOM
    end
  end

  def exec_sql_on_uri(sql, uri)
    command.send(:exec_sql_on_uri, sql, uri)
  end

  def run
    dump_restore_cmd
  end
end

