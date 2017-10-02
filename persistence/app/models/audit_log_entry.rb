
require 'csv'
require 'ft_modules'

class AuditLogEntry < ActiveRecord::Base

   include FtUtils

   set_table_name 'audit_log_entries'



   def after_initialize()
     begin
     rescue
     end
   end

   def display_name
      return "Audit Log Entry"
   end

   def self.display_name
      return "Audit Log Entry"
   end

   def ctrlr_name
      return "audit_log_entries"
   end

   def self.iattr_name
      return "snapshot_time"
   end

   def iattr_name
      return "snapshot_time"
   end

   def iname
      return "" if self.snapshot_time.nil? 
      return self.snapshot_time.to_s
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
          @@attr_info['snapshot_time'] = { :datatype => 'long'.downcase, :modifier => '', :is_identity_attr => true, :default => '', :mode => 'rw' }
          @@attr_info['action_name'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['category'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['user_name'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['target_object_class'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['target_object_name'] = { :datatype => 'string', :modifier => '', :length => 250, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['target_object_uuid'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['source_class'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['source_name'] = { :datatype => 'string', :modifier => '', :length => 250, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['source_uuid'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['destination_class'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['destination_name'] = { :datatype => 'string', :modifier => '', :length => 250, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['destination_uuid'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['details'] = { :datatype => 'text', :modifier => '', :is_identity_attr => false, :default => '', :mode => 'rw' }
        end
        return attr_name.nil? ? @@attr_info : @@attr_info[attr_name.to_s]
    end

    def self.attr_lengths
      { 'action_name' => 80, 'category' => 80, 'user_name' => 80, 'target_object_class' => 80, 'target_object_name' => 250, 'target_object_uuid' => 80, 'source_class' => 80, 'source_name' => 250, 'source_uuid' => 80, 'destination_class' => 80, 'destination_name' => 250, 'destination_uuid' => 80 }
    end

    def self.value_attrs
      ["snapshot_time","action_name","category","user_name","target_object_class","target_object_name","target_object_uuid","source_class","source_name","source_uuid","destination_class","destination_name","destination_uuid","details"]
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
       ["snapshot_time","action_name","category","user_name","target_object_class","target_object_name","target_object_uuid","source_class","source_name","source_uuid","destination_class","destination_name","destination_uuid","details"]
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
        unless row[:snapshot_time].to_s.empty? then
          obj = find_or_create_by_snapshot_time(row[:snapshot_time])

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
    item = AuditLogEntry.find(:first, :order => FtUtils.quoted_name('snapshot_time'), :conditions => ["snapshot_time > ? ", self.snapshot_time] )
    item = AuditLogEntry.find(:first, :order => FtUtils.quoted_name('snapshot_time')) if item.nil? 
    return item
  end

  def prev_item
    item = AuditLogEntry.find(:first, :order => FtUtils.quoted_name('snapshot_time') + ' DESC', :conditions => ["snapshot_time < ? ", self.snapshot_time] )
    item = AuditLogEntry.find(:first, :order => FtUtils.quoted_name('snapshot_time') + ' DESC') if item.nil? 
    return item
  end

  def self.items_for_index
    return AuditLogEntry.find(:all, :order => 'snapshot_time')
  end

  # ---

  def self.default_query(cond=nil)

    return <<-EOS
          (SELECT audit_log_entry.snapshot_time as #{FtUtils.quoted_name('snapshot_time')},
              audit_log_entry.action_name as #{FtUtils.quoted_name('action_name')},
              audit_log_entry.category as #{FtUtils.quoted_name('category')},
              audit_log_entry.user_name as #{FtUtils.quoted_name('user_name')},
              audit_log_entry.target_object_class as #{FtUtils.quoted_name('target_object_class')},
              audit_log_entry.target_object_name as #{FtUtils.quoted_name('target_object_name')},
              audit_log_entry.target_object_uuid as #{FtUtils.quoted_name('target_object_uuid')},
              audit_log_entry.source_class as #{FtUtils.quoted_name('source_class')},
              audit_log_entry.source_name as #{FtUtils.quoted_name('source_name')},
              audit_log_entry.source_uuid as #{FtUtils.quoted_name('source_uuid')},
              audit_log_entry.destination_class as #{FtUtils.quoted_name('destination_class')},
              audit_log_entry.destination_name as #{FtUtils.quoted_name('destination_name')},
              audit_log_entry.destination_uuid as #{FtUtils.quoted_name('destination_uuid')},
              audit_log_entry.details as #{FtUtils.quoted_name('details')},
              audit_log_entry.id as id
          FROM
              #{FtUtils.quoted_name('audit_log_entries')} AS #{FtUtils.quoted_name('audit_log_entry')}
          #{cond ? ("WHERE "+cond.to_s) : ""}
          ) AS audit_log_entries
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
        { :title => "List of "+"Audit Log Entry".pluralize,
          :sql  => "SELECT * FROM #{@@default_query}",
          :cols => AuditLogEntry.value_attrs.concat(AuditLogEntry.foreign_key_attrs),
          :where => "snapshot_time IS NOT NULL",
          :order_by => ['snapshot_time']
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
    return AuditLogEntry.find(:all, :order => 'snapshot_time')
  end


#BEGIN-UID.usermethods

  def self.default_query(cond="true")

    return <<-EOS
          SELECT audit_log_entry.snapshot_time as #{FtUtils.quoted_name('snapshot_time')},
              audit_log_entry.snapshot_time as #{FtUtils.quoted_name('date_time')},
              audit_log_entry.action_name as #{FtUtils.quoted_name('action_name')},
              audit_log_entry.category as #{FtUtils.quoted_name('category')},
              audit_log_entry.user_name as #{FtUtils.quoted_name('user_name')},
              audit_log_entry.target_object_class as #{FtUtils.quoted_name('target_object_class')},
              audit_log_entry.target_object_name as #{FtUtils.quoted_name('target_object_name')},
              audit_log_entry.target_object_uuid as #{FtUtils.quoted_name('target_object_uuid')},
              audit_log_entry.source_class as #{FtUtils.quoted_name('source_class')},
              audit_log_entry.source_name as #{FtUtils.quoted_name('source_name')},
              audit_log_entry.source_uuid as #{FtUtils.quoted_name('source_uuid')},
              audit_log_entry.destination_class as #{FtUtils.quoted_name('destination_class')},
              audit_log_entry.destination_name as #{FtUtils.quoted_name('destination_name')},
              audit_log_entry.destination_uuid as #{FtUtils.quoted_name('destination_uuid')},
              audit_log_entry.details as #{FtUtils.quoted_name('details')},
              audit_log_entry.id as id
          FROM
              #{FtUtils.quoted_name('audit_log_entries')} AS #{FtUtils.quoted_name('audit_log_entry')}
          EOS
  end

  def self.query_specs

    @@default_query = default_query().strip

    @@query_specs = {
      "list_all" =>
        { :title => "List of "+"Audit Log Entry".pluralize,
          :sql  => @@default_query, # "SELECT * FROM #{@@default_query}",
          :cols => AuditLogEntry.value_attrs.concat(AuditLogEntry.foreign_key_attrs),
          :where => nil, # "snapshot_time IS NOT NULL",
          :order_by => ['snapshot_time']
        }
        # , separate query specs by commas
    }

    # set the name of the query in each query spec
    @@query_specs.each{|k,v| v[:name] = k}

    return @@query_specs
  end

  # used in showing query results; gets around method name collisions; returns derived attr values
  def get_attr_val(ft_col_name,ctx='html')
    return Time.at(self.snapshot_time.to_i/1000).to_datetime_image if ft_col_name == "date_time"

    return read_attribute(ft_col_name)
  end

  def to_xml(options = {}, &block)
    xml = options[:builder] || ::Builder::XmlMarkup.new
    xml.tag!('audit-log-entry', ({'href' => "/audit_log_entries/#{self.id}.xml"})) {
      for attr in AuditLogEntry.value_attrs do
        if attr == "snapshot_time"
          xml.tag! 'created-at', self.snapshot_time.to_s
        else
          xml.tag! attr.gsub("_","-"), self.get_attr_val(attr)
        end
      end
    }
  end

#END-UID.usermethods

end
