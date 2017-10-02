
require 'rubygems'
require 'rexml/document'
require 'fileutils'
require 'yaml'
require 'date'

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
#   run this script whenever the persistence server starts
#
#     ruby /srv/rails/webapps/persistence/script/apply_db_config.rb
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

class DBConfigOpts

  def initialize
    @config_file_path = "/srv/tomcat6/data/config/retention.config.topology"

    @adapter    = 'mysql'
	
    @port_no    = '1433'
    @host_name  = '' # '10.10.99.201'
	
	@birt_version = 'birt25'
	
    begin
      xml         = File.open(@config_file_path, "rb"){|f| f.read}
      doc         = REXML::Document.new(xml.to_s)

      root        = doc.root
      attrs       = root.attributes
      @adapter    = (attrs['adapter']    ? attrs['adapter']    : @adapter)
      @port_no    = (attrs['portNumber'] ? attrs['portNumber'] : @port_no)
      @host_name  = (attrs['hostName']   ? attrs['hostName']   : @host_name)
      
      
      
    rescue => e
      puts e.message
      doc = root = nil
    end

    @use_sql_server = (@adapter == "sqlserver")

    @persistence_config_dir = "/srv/rails/webapps/persistence/config"

    @reports_dir   = "/srv/reports"
    @sqlserver_dir = "#{@reports_dir}/SQLServer"
    @mysql_dir     = "#{@reports_dir}/MySQL"
    
    begin
		birt = `rpm -qa birt-runtime*`
		p birt
	    @birt_version = (/2\.5/ =~ birt) ? 'birt25' : 'birt42'
	    p @birt_version
    rescue => e
    end
      
  end
  
  def birt_version
    return @birt_version
  end

  def apply_sqlserver_mode
    return @use_sql_server
  end

  def persistence_config_dir
    return @persistence_config_dir
  end

  def reports_dir
    return @reports_dir
  end

  def sqlserver_dir
    return @sqlserver_dir
  end

  def mysql_dir
    return @mysql_dir
  end

  def host_name_or_ip
    return @host_name
  end

  def port_no
    return @port_no
  end

  def sqlserver_jdbc_string
    jdbc_string = "jdbc:sqlserver://#{@host_name}:#{@port_no};databaseName=vmtdb;integratedSecurity=false;"
    return jdbc_string
  end
  
end


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


class ConfigurationMethods

  def initialize(config_opts)
    @db_config_opts = config_opts
  end

  def configure_sqlserver_reports
    begin
      jdbc_str = @db_config_opts.sqlserver_jdbc_string

      sqlserver_dir = @db_config_opts.sqlserver_dir
      subdirs = ["VmtReports", "VmtReportTemplates"]

      for subdir in subdirs
        dir_path = sqlserver_dir + "/" + subdir

        Dir.glob(dir_path+"/*.rptdesign") { |fpath|
  #        puts "configuring #{fpath}"

          file_contents = File.open(fpath,"rb"){|f| f.read}

          # replace <property name="odaURL">...</property>
          file_contents = file_contents.gsub(/<property name="odaURL">.*<\/property>/,
                                             "<property name=\"odaURL\">#{jdbc_str}</property>")

          File.open(fpath, "wb"){|f| f.write(file_contents)}
        }
      end

    rescue Exception => ex
      puts ex.message
      puts ex.backtrace.join("\n")

    end
  end


  # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

  def apply_links(target_dir)

    # remove existing links
    links = ["VmtReports", "VmtReportTemplates"]

    for link_name in links do
      link_path = "#{@db_config_opts.reports_dir}/#{link_name}"

      if File.exists?(link_path) then
        File.delete(link_path)
      end

      target_path = "#{target_dir}/#{@db_config_opts.birt_version}/#{link_name}"
	  
	  system("rm #{link_path}")
      system("ln -s #{target_path} #{link_path}")
    end
  end


  # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

  def configure_persistence(target)
    config_dir = @db_config_opts.persistence_config_dir

    in_file = "database.in.yml"
    out_file = "database.yml"

    in_yaml  = YAML.load_file("#{config_dir}/#{in_file}")
    out_yaml = in_yaml.clone

    if target == 'mysql' then

      out_yaml['production'] = out_yaml['mysql'].clone
      out_yaml['development'] = out_yaml['mysql'].clone

    elsif target == 'sqlserver' then

      out_yaml['mssqlserver']['dataserver'] = @db_config_opts.host_name_or_ip

      out_yaml['production'] = out_yaml['mssqlserver'].clone
      out_yaml['development'] = out_yaml['mssqlserver'].clone

    end

    File.open("#{config_dir}/#{out_file}", "wb") { |f|
      f.write(out_yaml.to_yaml().gsub(/---\s*\n/,"\n").gsub(/\n([a-z])/,"\n\n\\1"))
    }

  end

  # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

  def configure_reporting(target)
    reports_bin_dir = "#{@db_config_opts.reports_dir}/bin"

    in_file = "run_all.in.sh"
    out_file = "run_all.sh"

    in_script  = File.open("#{reports_bin_dir}/#{in_file}","rb"){|f| f.read}

    if target == 'mysql' then

      out_script = in_script

    elsif target == 'sqlserver' then

      server = @db_config_opts.host_name_or_ip

      out_script = in_script.gsub("###tsql","tsql").gsub("$(SERVER)",server)

    end

    File.open("#{reports_bin_dir}/#{out_file}", "wb") { |f|
      f.write(out_script)
    }

  end

end


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
#   Main
#

config_opts    = DBConfigOpts.new()
config_methods = ConfigurationMethods.new(config_opts)

if config_opts.apply_sqlserver_mode then
  config_methods.configure_sqlserver_reports()
  config_methods.apply_links(config_opts.sqlserver_dir)
  config_methods.configure_persistence('sqlserver')
  config_methods.configure_reporting('sqlserver')

else
  config_methods.apply_links(config_opts.mysql_dir)
  config_methods.configure_persistence('mysql')
  config_methods.configure_reporting('mysql')

end
