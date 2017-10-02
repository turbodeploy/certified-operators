
require 'csv'
require 'ft_modules'

class StandardReport < ActiveRecord::Base

   include FtUtils

   set_table_name 'standard_reports'

   has_many             :report_subscriptions, :class_name => "ReportSubscription", :foreign_key => "standard_report_standard_report_id"



   def after_initialize()
     begin
     rescue
     end
   end

   def display_name
      return "Standard Report"
   end

   def self.display_name
      return "Standard Report"
   end

   def ctrlr_name
      return "standard_reports"
   end

   def self.iattr_name
      return "filename"
   end

   def iattr_name
      return "filename"
   end

   def iname
      return "" if self.filename.nil? 
      return self.filename.to_s
   end

   def qname
      q_name = iname()
      return q_name
   end

   def sname
       return self.qname
   end

   def container_qname
      return ""
   end

   def container_obj
      return nil 
   end

   def get_children
      # override this method to return this object's sub-items - used in "tree_for" helper

      # return [ { :title => "Title", :items => [ item, item, ... ] }, ... ] to allow the tree to show multiple titled lists of sub-items
      # return [ item, item, ... ] to allow the tree to show a simple indented list of sub-items without a title

      sub_items = [ ]
      return sub_items
   end



    # runtime metadata support methods

    def self.attr_type_info(attr_name=nil)
        unless defined?(@@attr_info)
          @@attr_info = {}
          @@attr_info['filename'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => true, :default => '', :mode => 'rw' }
          @@attr_info['title'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['category'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['short_desc'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['description'] = { :datatype => 'text', :modifier => '', :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['period'] = { :datatype => 'string', :modifier => 'enum', :length => 80, :is_identity_attr => false, :default => '', :values => 'Daily,Weekly,Monthly', :mode => 'rw' }
          @@attr_info['day_type'] = { :datatype => 'string', :modifier => 'enum', :length => 80, :is_identity_attr => false, :default => '', :values => 'Mon,Tue,Wed,Thu,Fri,Sat,Sun', :mode => 'rw' }
        end
        return attr_name.nil? ? @@attr_info : @@attr_info[attr_name.to_s]
    end

    def self.attr_lengths
      { 'filename' => 80, 'title' => 80, 'category' => 80, 'short_desc' => 80, 'period' => 80, 'day_type' => 80 }
    end

    def self.value_attrs
      ["filename","title","category","short_desc","description","period","day_type"]
    end

    def self.foreign_key_attrs
       []
    end

    def self.foreign_key_attr_info(fk_attr_name=nil)
      unless defined?(@@fk_attr_info)
        @@fk_attr_info = { }
      end

      return fk_attr_name.nil? ? @@fk_attr_info : @@fk_attr_info[fk_attr_name.to_s]
    end

    def self.get_form_fields()
       ["filename","title","category","short_desc","description","period","day_type"]
    end

    def self.csv_header_row
      (value_attrs + foreign_key_attrs + []).join(",")
    end

    def self.load_csv_file(filename)
        self.load_csv_str(File.read(filename))
    end

    def self.load_csv_str(csv_str)
        rows = load_csv_data(csv_str)
        update_table_entries(rows)
    end

    def self.load_csv_data(csv_str)
        headers = Array.new
        rows = Array.new
        row_no = 1

        cname = String.new
        cval = String.new

        records = CSV.parse(csv_str) || []

        records.each do |row|
            if row
                if row_no == 1
                    headers = row.collect{|v| v.to_s.strip.gsub(/\s+/,"").underscore}
                else
                    row_data = Hash.new("")
                    col_no = 0
                    row.each { |val|
                      if ! headers[col_no].to_s.empty?
                        cname = headers[col_no]
                        cval = val.to_s.gsub("\"","").strip

                        limit = attr_lengths[cname]
                        cval = cval[0...limit] if limit

                        begin
                          row_data[cname.to_sym] = cval.gsub(/[\x80-\xff]/,'')
                        rescue
                        end
                      end
                      col_no = col_no + 1
                    }
                    rows << row_data
                end

                row_no = row_no + 1
            end
        end

        return rows 
    end

    def self.update_table_entries(rows)
      rows.each { |row|
        unless row[:filename].to_s.empty? then
          obj = find_or_create_by_filename(row[:filename])

          attrs = row.clone
          attrs.delete_if { |k,v| (! self.value_attrs.member? k.to_s) || v.size == 0 }
          obj.attributes = attrs


          obj.save()
        end # if identity attr supplied
      }
    end

 public


  # returns the next/prev item given its context of ownership: a) global or b) as a contained item of another object

  def next_item
    item = StandardReport.find(:first, :order => FtUtils.quoted_name('filename'), :conditions => ["filename > ? ", self.filename] )
    item = StandardReport.find(:first, :order => FtUtils.quoted_name('filename')) if item.nil? 
    return item
  end

  def prev_item
    item = StandardReport.find(:first, :order => FtUtils.quoted_name('filename') + ' DESC', :conditions => ["filename < ? ", self.filename] )
    item = StandardReport.find(:first, :order => FtUtils.quoted_name('filename') + ' DESC') if item.nil? 
    return item
  end

  def self.items_for_index
    return StandardReport.find(:all, :order => 'filename')
  end

  # ---

  def self.default_query(cond=nil)

    return <<-EOS
          (SELECT standard_report.filename as #{FtUtils.quoted_name('filename')},
              standard_report.title as #{FtUtils.quoted_name('title')},
              standard_report.category as #{FtUtils.quoted_name('category')},
              standard_report.short_desc as #{FtUtils.quoted_name('short_desc')},
              standard_report.description as #{FtUtils.quoted_name('description')},
              standard_report.period as #{FtUtils.quoted_name('period')},
              standard_report.day_type as #{FtUtils.quoted_name('day_type')},
              standard_report.id as id
          FROM
              #{FtUtils.quoted_name('standard_reports')} AS #{FtUtils.quoted_name('standard_report')}
          #{cond ? ("WHERE "+cond.to_s) : ""}
          ) AS standard_reports
          EOS
  end

  def self.query_specs

    # override this method to define query specs - an example is provided
    #   :title - the query title for display
    #   :sql   - the query
    #   :cols  - array mapping out the order in which the columns should appear left-to-right
    #   :where - an expression to place after a where clause in the query
    #   :order_by - comma separated list of column names in array brackets [ ... ]

    @@default_query = default_query()

    @@query_specs = {
      "list_all" => 
        { :title => "List of "+"Standard Report".pluralize,
          :sql  => "SELECT * FROM #{@@default_query}",
          :cols => StandardReport.value_attrs.concat(StandardReport.foreign_key_attrs),
          :where => "filename IS NOT NULL",
          :order_by => ['filename']
        }
        # , separate query specs by commas
    }

    # set the name of the query in each query spec
    @@query_specs.each{|k,v| v[:name] = k}

    return @@query_specs
  end

  # used in showing query results; gets around method name collisions; returns derived attr values
  def get_attr_val(ft_col_name,ctx='html')
    # use the least expensive means for html contexts (lists and queries); do full eval for others (e.g. csv)
    return read_attribute(ft_col_name)
  end

  def self.choices_for(obj,rel_name=nil)
    return StandardReport.find(:all, :order => 'filename')
  end


#BEGIN-UID.usermethods

  def get_attr_val(ft_col_name,ctx='html')
    case ft_col_name
      when 'day_type'
        return self.day_type
      else
        return read_attribute(ft_col_name)
    end
  end


  def day_type
    rval = read_attribute('day_type').to_s
    return "Sat" if self.period.to_s == 'Weekly' && rval.empty?
    return rval
  end


  def before_destroy
    self.report_subscriptions.destroy_all()
  end


  def is_obsolete?
    return false
  end


  def iname
    return self.title || self.filename
  end


  def category
    c = self.read_attribute(:category)
    c = "Utilization" if c.nil? || c.to_s.empty?
    return c
  end


  def to_xml(options = {}, &block)
    xml = options[:builder] || ::Builder::XmlMarkup.new

    host_repts = StandardReport.host_feature_enabled?
    storage_repts = StandardReport.storage_feature_enabled?
    planner_repts = StandardReport.planner_feature_enabled?

    licensed = (host_repts && self.is_a_host_report?) ||
               (storage_repts && self.is_a_storage_report?) ||
               (planner_repts && self.is_a_planner_report?)

    attrs = {
              'href'     => "/persistence/standard_reports/#{self.id}.pdf?date=MostRecent",
              'licensed' => licensed ? "true" : "false"
            }

    for attr in StandardReport.value_attrs do
      next if attr == 'description' || attr == 'filename'

      attrs[attr.gsub("_","-")] = self.get_attr_val(attr,'xml')
    end

    xml.tag!('report', attrs) {
      xml.description self.description.to_s
      unless options[:no_dates]
        xml.tag! 'report-dates', :type => 'array' do
          for yyyy_mm_dd in Dir.new("#{ENV['VMT_REPORTS_HOME']}/pdf_files")
            next unless yyyy_mm_dd =~ /\d+-\d+-\d+/ && File.exists?("#{ENV['VMT_REPORTS_HOME']}/pdf_files/#{yyyy_mm_dd}/#{self.filename}.pdf")
            xml.tag! 'report-date', yyyy_mm_dd
          end
        end
      end
    }
  end


  def dates_available
    dates = []
    for yyyy_mm_dd in Dir.new("#{ENV['VMT_REPORTS_HOME']}/pdf_files")
      
      report_exists = REPORT_EXTS_W_CMPED.inject(false) do |exists, ext| 
        exists ||= File.exists?("#{ENV['VMT_REPORTS_HOME']}/pdf_files/#{yyyy_mm_dd}/#{self.filename}.#{ext}") 
      end

      if yyyy_mm_dd =~ /\d+-\d+-\d+/ && report_exists
        dates << yyyy_mm_dd
      end
    end

    return dates.sort
  end

  # ---

  def self.reports_available?
    for yyyy_mm_dd in Dir.new("#{ENV['VMT_REPORTS_HOME']}/pdf_files")
      next unless yyyy_mm_dd =~ /\d+-\d+-\d+/
      return true
    end
    return false
  end

  # ---

  def self.host_feature_enabled?
    return true   # always enabled - but keep this logic in case we change the rules

#    lic_desc = Entity.get_license_descriptor
#    if lic_desc
#      repts_ind = lic_desc.get_attr_val("hostReportsEnabled").to_s
#      return repts_ind.empty? ? false : (repts_ind == 'true')
#    else
#      return false
#    end
  end

  def self.storage_feature_enabled?
    return true   # always enabled - but keep this logic in case we change the rules

#    lic_desc = Entity.get_license_descriptor
#    if lic_desc
#      repts_ind = lic_desc.get_attr_val("storageReportsEnabled").to_s
#      return repts_ind.empty? ? false : (repts_ind == 'true')
#    else
#      return false
#    end
  end

  def self.planner_feature_enabled?
    return true   # always enabled - but keep this logic in case we change the rules

#    lic_desc = Entity.get_license_descriptor
#    if lic_desc
#      repts_ind = lic_desc.get_attr_val("planEnabled").to_s
#      return repts_ind.empty? ? false : (repts_ind == 'true')
#    else
#      return false
#    end
  end

  def self.custom_reports_feature_enabled?
    lic_desc = Entity.get_license_descriptor
    if lic_desc
      repts_ind = lic_desc.get_attr_val("customReportsEnabled").to_s
      return repts_ind.empty? ? false : (repts_ind == 'true')
    else
      return false
    end
  end

  # ---

  def is_a_host_report?
    return ! self.filename.to_s.downcase["storage"] &&
           ! self.filename.to_s.downcase["rightsizing"]
  end

  def is_a_storage_report?
    return self.filename.to_s.downcase["storage"]
  end

  def is_a_planner_report?
    return self.filename.to_s.downcase["rightsizing"]
  end

  def user_selected?
    rept_config_mgr = Entity.find_by_name("ReportingConfigManager")
    if rept_config_mgr
      selected_repts = rept_config_mgr.get_attr_val("selectedReports")
      if selected_repts
        rept_ids = selected_repts.split(",").collect{|href| href[/\d+/]}.compact 
        return rept_ids.include?(self.id.to_s)
      end
    end

    logger.error "Error: System entity ReportingConfigManager not found; no reports enabled"
    return false
  end

  # # #

  def run_today?
    period = self.period.to_s.downcase

    case period
      when "daily"
        return true

      when "weekly"
        day_names = self.day_type.to_s.strip.downcase
        todays_name = Time.now.strftime("%a").downcase
        return day_names.include?(todays_name)

      when "monthly"
        return Date.today.day == 1

      else
        return true
    end
  end


  # test points - uncomment to effect the outcome of the question

#  def self.host_feature_enabled?
#    return true
#  end
#
#  def self.storage_feature_enabled?
#    return true
#  end
#
#  def self.planner_feature_enabled?
#    return true
#  end
#
#  def user_selected?
#    true
#  end


#END-UID.usermethods

end
