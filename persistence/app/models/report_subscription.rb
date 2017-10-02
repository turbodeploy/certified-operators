
require 'csv'
require 'ft_modules'

class ReportSubscription < ActiveRecord::Base

   include FtUtils

   set_table_name 'report_subscriptions'

   belongs_to           :standard_report, :class_name => "StandardReport", :foreign_key => "standard_report_standard_report_id"

   belongs_to           :custom_report, :class_name => "UserReport", :foreign_key => "custom_report_user_report_id"

   belongs_to           :on_demand_report, :class_name => "OnDemandReport", :foreign_key => "on_demand_report_on_demand_report_id"



   validates_format_of :email, :with => /^(|(([A-Za-z0-9]+_+)|([A-Za-z0-9]+\-+)|([A-Za-z0-9]+\.+)|([A-Za-z0-9]+\++))*[A-Za-z0-9]+@((\w+\-+)|(\w+\.))*\w{1,63}\.[a-zA-Z]{2,6})$/i


   def after_initialize()
     begin
     rescue
     end
   end

   def display_name
      return "Report Subscription"
   end

   def self.display_name
      return "Report Subscription"
   end

   def ctrlr_name
      return "report_subscriptions"
   end

   def self.iattr_name
      return "email"
   end

   def iattr_name
      return "email"
   end

   def iname
      return "" if self.email.nil? 
      return self.email.to_s
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
          @@attr_info['email'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => true, :default => '', :mode => 'rw' }
          @@attr_info['period'] = { :datatype => 'string', :modifier => 'enum', :length => 80, :is_identity_attr => false, :default => '', :values => 'Daily,Weekly,Monthly', :mode => 'rw' }
          @@attr_info['day_type'] = { :datatype => 'string', :modifier => 'enum', :length => 80, :is_identity_attr => false, :default => '', :values => 'Mon,Tue,Wed,Thu,Fri,Sat,Sun', :mode => 'rw' }
          @@attr_info['obj_uuid'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
        end
        return attr_name.nil? ? @@attr_info : @@attr_info[attr_name.to_s]
    end

    def self.attr_lengths
      { 'email' => 80, 'period' => 80, 'day_type' => 80, 'obj_uuid' => 80 }
    end

    def self.value_attrs
      ["email","period","day_type","obj_uuid"]
    end

    def self.foreign_key_attrs
       ["standard_report","custom_report","on_demand_report"]
    end

    def self.foreign_key_attr_info(fk_attr_name=nil)
      unless defined?(@@fk_attr_info)
        @@fk_attr_info = { }
        @@fk_attr_info['standard_report'] = {:datatype => "StandardReport", :foreign_key => "standard_report_standard_report_id"}
        @@fk_attr_info['custom_report'] = {:datatype => "UserReport", :foreign_key => "custom_report_user_report_id"}
        @@fk_attr_info['on_demand_report'] = {:datatype => "OnDemandReport", :foreign_key => "on_demand_report_on_demand_report_id"}
      end

      return fk_attr_name.nil? ? @@fk_attr_info : @@fk_attr_info[fk_attr_name.to_s]
    end

    def self.get_form_fields()
       ["email","period","day_type","obj_uuid"]
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
        unless row[:email].to_s.empty? then
          obj = find_or_create_by_email(row[:email])

          attrs = row.clone
          attrs.delete_if { |k,v| (! self.value_attrs.member? k.to_s) || v.size == 0 }
          obj.attributes = attrs

          obj.standard_report = StandardReport.find_or_create_by_filename(row[:standard_report]) unless row[:standard_report].to_s.empty?
          obj.custom_report = UserReport.find_or_create_by_title(row[:custom_report]) unless row[:custom_report].to_s.empty?
          obj.on_demand_report = OnDemandReport.find_or_create_by_filename(row[:on_demand_report]) unless row[:on_demand_report].to_s.empty?

          obj.save()
        end # if identity attr supplied
      }
    end

 public


  # returns the next/prev item given its context of ownership: a) global or b) as a contained item of another object

  def next_item
    item = ReportSubscription.find(:first, :order => FtUtils.quoted_name('email'), :conditions => ["email > ? ", self.email] )
    item = ReportSubscription.find(:first, :order => FtUtils.quoted_name('email')) if item.nil? 
    return item
  end

  def prev_item
    item = ReportSubscription.find(:first, :order => FtUtils.quoted_name('email') + ' DESC', :conditions => ["email < ? ", self.email] )
    item = ReportSubscription.find(:first, :order => FtUtils.quoted_name('email') + ' DESC') if item.nil? 
    return item
  end

  def self.items_for_index
    return ReportSubscription.find(:all, :order => 'email')
  end

  # ---

  def self.default_query(cond=nil)

    return <<-EOS
          (SELECT report_subscription.email as #{FtUtils.quoted_name('email')},
              report_subscription.period as #{FtUtils.quoted_name('period')},
              report_subscription.day_type as #{FtUtils.quoted_name('day_type')},
              report_subscription.obj_uuid as #{FtUtils.quoted_name('obj_uuid')},
              standard_report.filename as #{FtUtils.quoted_name('standard_report')},
              standard_report.id as standard_report_standard_report_id,
              custom_report.title as #{FtUtils.quoted_name('custom_report')},
              custom_report.id as custom_report_user_report_id,
              on_demand_report.filename as #{FtUtils.quoted_name('on_demand_report')},
              on_demand_report.id as on_demand_report_on_demand_report_id,
              report_subscription.id as id
          FROM
              #{FtUtils.quoted_name('report_subscriptions')} AS #{FtUtils.quoted_name('report_subscription')}
              left outer join #{FtUtils.quoted_name('standard_reports')} as #{FtUtils.quoted_name('standard_report')} on report_subscription.standard_report_standard_report_id = standard_report.id
              left outer join #{FtUtils.quoted_name('user_reports')} as #{FtUtils.quoted_name('custom_report')} on report_subscription.custom_report_user_report_id = custom_report.id
              left outer join #{FtUtils.quoted_name('on_demand_reports')} as #{FtUtils.quoted_name('on_demand_report')} on report_subscription.on_demand_report_on_demand_report_id = on_demand_report.id
          #{cond ? ("WHERE "+cond.to_s) : ""}
          ) AS report_subscriptions
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
        { :title => "List of "+"Report Subscription".pluralize,
          :sql  => "SELECT * FROM #{@@default_query}",
          :cols => ReportSubscription.value_attrs.concat(ReportSubscription.foreign_key_attrs),
          :where => "email IS NOT NULL",
          :order_by => ['email']
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
    return ReportSubscription.find(:all, :order => 'email')
  end


#BEGIN-UID.usermethods

  def get_report
    return self.standard_report || self.custom_report || self.on_demand_report || nil
  end

  def is_obsolete?
    rept = self.get_report
    return true if rept.nil?

    return rept.is_obsolete?
  end

#END-UID.usermethods

end
