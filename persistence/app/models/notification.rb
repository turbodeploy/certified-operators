
require 'csv'
require 'ft_modules'

class Notification < ActiveRecord::Base

   include FtUtils

   set_table_name 'notifications'



   def after_initialize()
     begin
     rescue
     end
   end

   def display_name
      return "Notification"
   end

   def self.display_name
      return "Notification"
   end

   def ctrlr_name
      return "notifications"
   end

   def self.iattr_name
      return "clear_time"
   end

   def iattr_name
      return "clear_time"
   end

   def iname
      return "" if self.clear_time.nil? 
      return self.clear_time.to_s
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
          @@attr_info['clear_time'] = { :datatype => 'long'.downcase, :modifier => '', :is_identity_attr => true, :default => '', :mode => 'rw' }
          @@attr_info['last_notify_time'] = { :datatype => 'long'.downcase, :modifier => '', :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['severity'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['category'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['name'] = { :datatype => 'string', :modifier => '', :length => 250, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['uuid'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['importance'] = { :datatype => 'double'.downcase, :modifier => '', :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['description'] = { :datatype => 'text', :modifier => '', :is_identity_attr => false, :default => '', :mode => 'rw' }
        end
        return attr_name.nil? ? @@attr_info : @@attr_info[attr_name.to_s]
    end

    def self.attr_lengths
      { 'severity' => 80, 'category' => 80, 'name' => 250, 'uuid' => 80 }
    end

    def self.value_attrs
      ["clear_time","last_notify_time","severity","category","name","uuid","importance","description"]
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
       ["clear_time","last_notify_time","severity","category","name","uuid","importance","description"]
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
        unless row[:clear_time].to_s.empty? then
          obj = find_or_create_by_clear_time(row[:clear_time])

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
    item = Notification.find(:first, :order => FtUtils.quoted_name('clear_time'), :conditions => ["clear_time > ? ", self.clear_time] )
    item = Notification.find(:first, :order => FtUtils.quoted_name('clear_time')) if item.nil? 
    return item
  end

  def prev_item
    item = Notification.find(:first, :order => FtUtils.quoted_name('clear_time') + ' DESC', :conditions => ["clear_time < ? ", self.clear_time] )
    item = Notification.find(:first, :order => FtUtils.quoted_name('clear_time') + ' DESC') if item.nil? 
    return item
  end

  def self.items_for_index
    return Notification.find(:all, :order => 'clear_time')
  end

  # ---

  def self.default_query(cond=nil)

    return <<-EOS
          (SELECT notification.clear_time as #{FtUtils.quoted_name('clear_time')},
              notification.last_notify_time as #{FtUtils.quoted_name('last_notify_time')},
              notification.severity as #{FtUtils.quoted_name('severity')},
              notification.category as #{FtUtils.quoted_name('category')},
              notification.name as #{FtUtils.quoted_name('name')},
              notification.uuid as #{FtUtils.quoted_name('uuid')},
              notification.importance as #{FtUtils.quoted_name('importance')},
              notification.description as #{FtUtils.quoted_name('description')},
              notification.id as id
          FROM
              #{FtUtils.quoted_name('notifications')} AS #{FtUtils.quoted_name('notification')}
          #{cond ? ("WHERE "+cond.to_s) : ""}
          ) AS notifications
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
        { :title => "List of "+"Notification".pluralize,
          :sql  => "SELECT * FROM #{@@default_query}",
          :cols => Notification.value_attrs.concat(Notification.foreign_key_attrs),
          :where => "clear_time IS NOT NULL",
          :order_by => ['clear_time']
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
    return Notification.find(:all, :order => 'clear_time')
  end


#BEGIN-UID.usermethods

  def get_attr_val(ft_col_name,ctx='html')
    # use the least expensive means for html contexts (lists and queries); do full eval for others (e.g. csv)
    if ft_col_name == "htime"
      return Time.at(self.clear_time/1000).to_datetime_image()
    end

    return read_attribute(ft_col_name)
  end

#END-UID.usermethods

end
