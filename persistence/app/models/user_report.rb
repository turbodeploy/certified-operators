
require 'csv'
require 'ft_modules'

class UserReport < ActiveRecord::Base

   include FtUtils

   set_table_name 'user_reports'

   has_many             :report_subscriptions, :class_name => "ReportSubscription", :foreign_key => "custom_report_user_report_id"



   def after_initialize()
     begin
     rescue
     end
   end

   def display_name
      return "User Report"
   end

   def self.display_name
      return "User Report"
   end

   def ctrlr_name
      return "user_reports"
   end

   def self.iattr_name
      return "title"
   end

   def iattr_name
      return "title"
   end

   def iname
      return "" if self.title.nil? 
      return self.title.to_s
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
          @@attr_info['title'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => true, :default => '', :mode => 'rw' }
          @@attr_info['category'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['short_desc'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['description'] = { :datatype => 'text', :modifier => '', :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['xml_descriptor'] = { :datatype => 'text', :modifier => '', :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['period'] = { :datatype => 'string', :modifier => 'enum', :length => 80, :is_identity_attr => false, :default => '', :values => 'Daily,Weekly,Monthly', :mode => 'rw' }
          @@attr_info['day_type'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
        end
        return attr_name.nil? ? @@attr_info : @@attr_info[attr_name.to_s]
    end

    def self.attr_lengths
      { 'title' => 80, 'category' => 80, 'short_desc' => 80, 'period' => 80, 'day_type' => 80 }
    end

    def self.value_attrs
      ["title","category","short_desc","description","xml_descriptor","period","day_type"]
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
       ["title","category","short_desc","description","xml_descriptor","period","day_type"]
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
        unless row[:title].to_s.empty? then
          obj = find_or_create_by_title(row[:title])

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
    item = UserReport.find(:first, :order => FtUtils.quoted_name('title'), :conditions => ["title > ? ", self.title] )
    item = UserReport.find(:first, :order => FtUtils.quoted_name('title')) if item.nil? 
    return item
  end

  def prev_item
    item = UserReport.find(:first, :order => FtUtils.quoted_name('title') + ' DESC', :conditions => ["title < ? ", self.title] )
    item = UserReport.find(:first, :order => FtUtils.quoted_name('title') + ' DESC') if item.nil? 
    return item
  end

  def self.items_for_index
    return UserReport.find(:all, :order => 'title')
  end

  # ---

  def self.default_query(cond=nil)

    return <<-EOS
          (SELECT user_report.title as #{FtUtils.quoted_name('title')},
              user_report.category as #{FtUtils.quoted_name('category')},
              user_report.short_desc as #{FtUtils.quoted_name('short_desc')},
              user_report.description as #{FtUtils.quoted_name('description')},
              user_report.xml_descriptor as #{FtUtils.quoted_name('xml_descriptor')},
              user_report.period as #{FtUtils.quoted_name('period')},
              user_report.day_type as #{FtUtils.quoted_name('day_type')},
              user_report.id as id
          FROM
              #{FtUtils.quoted_name('user_reports')} AS #{FtUtils.quoted_name('user_report')}
          #{cond ? ("WHERE "+cond.to_s) : ""}
          ) AS user_reports
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
        { :title => "List of "+"User Report".pluralize,
          :sql  => "SELECT * FROM #{@@default_query}",
          :cols => UserReport.value_attrs.concat(UserReport.foreign_key_attrs),
          :where => "title IS NOT NULL",
          :order_by => ['title']
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
    return UserReport.find(:all, :order => 'title')
  end


#BEGIN-UID.usermethods

  def before_destroy
    self.report_subscriptions.destroy_all()
  end


  def is_obsolete?
    xml = self.xml_descriptor.to_s
    return xml["<table>snapshots"]
  end

  # # #

  def run_today?
    period = self.period.to_s.downcase

    case period
      when "daily"
        return true

      when "weekly"
        todays_name = Time.now.strftime("%a").downcase
        day_types = self.day_type.to_s.strip.downcase
        day_types = "sat" if day_types.empty?
        return day_types.include?(todays_name)

      when "monthly"
        return Date.today.day == 1

      else
        return true
    end
  end

  # # #

  require 'rexml/document'

  def to_xml(options = {}, &block)

    doc = REXML::Document.new(self.xml_descriptor.to_s)
    root = doc.root

    str_title = root.elements["./title"].get_text.to_s
    str_table = root.elements["./table"].get_text.to_s

    xml_fields = root.get_elements("./fields/field")
    arr_fields = xml_fields.collect{|f| f.get_text.to_s}

    arr_filters = []
    xml_filters = root.get_elements("./filter_by/filter")
    xml_filters.each{|filter|
      arr_filters << [ filter.elements["./field"].get_text.to_s,
                       filter.elements["./op"].get_text.to_s,
                       filter.elements["./value"].get_text.to_s ]
    }

    arr_orders = []
    xml_orders = root.get_elements("./order_by/order")
    xml_orders.each{|order|
      arr_orders << [ order.elements["./field"].get_text.to_s,
                      order.elements["./direction"].get_text.to_s ]
    }

    # ---

    xml = options[:builder] || ::Builder::XmlMarkup.new
    xml.instruct! unless options[:skip_instruct]
    xml.user_report {
      xml.id self.id
      xml.title str_title
      xml.table str_table
      xml.fields {
        for str_field in arr_fields do
          xml.field str_field
        end
      }
      xml.filter_by {
        for filter in arr_filters do
          xml.filter {
            xml.field filter[0]
            xml.op filter[1]
            xml.value filter[2]
          }
        end
      }
      xml.order_by {
        for order in arr_orders
          xml.order {
            xml.field order[0]
            xml.direction order[1]
          }
        end
      }
    }

  end

#END-UID.usermethods

end
