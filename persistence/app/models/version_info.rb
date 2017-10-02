
require 'csv'
require 'ft_modules'

class VersionInfo < ActiveRecord::Base

   include FtUtils

   set_table_name 'version_info'



   def after_initialize()
     begin
     rescue
     end
   end

   def display_name
      return "Version Info"
   end

   def self.display_name
      return "Version Info"
   end

   def ctrlr_name
      return "version_infos"
   end

   def self.iattr_name
      return "version"
   end

   def iattr_name
      return "version"
   end

   def iname
      return "" if self.version.nil? 
      return self.version.to_s
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
          @@attr_info['version'] = { :datatype => 'int'.downcase, :modifier => '', :is_identity_attr => true, :default => '', :mode => 'rw' }
        end
        return attr_name.nil? ? @@attr_info : @@attr_info[attr_name.to_s]
    end

    def self.attr_lengths
      {  }
    end

    def self.value_attrs
      ["version"]
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
       ["version"]
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
        unless row[:version].to_s.empty? then
          obj = find_or_create_by_version(row[:version])

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
    item = VersionInfo.find(:first, :order => FtUtils.quoted_name('version'), :conditions => ["version > ? ", self.version] )
    item = VersionInfo.find(:first, :order => FtUtils.quoted_name('version')) if item.nil? 
    return item
  end

  def prev_item
    item = VersionInfo.find(:first, :order => FtUtils.quoted_name('version') + ' DESC', :conditions => ["version < ? ", self.version] )
    item = VersionInfo.find(:first, :order => FtUtils.quoted_name('version') + ' DESC') if item.nil? 
    return item
  end

  def self.items_for_index
    return VersionInfo.find(:all, :order => 'version')
  end

  # ---

  def self.default_query(cond=nil)

    return <<-EOS
          (SELECT version_info.version as #{FtUtils.quoted_name('version')},
              version_info.id as id
          FROM
              #{FtUtils.quoted_name('version_info')} AS #{FtUtils.quoted_name('version_info')}
          #{cond ? ("WHERE "+cond.to_s) : ""}
          ) AS version_infos
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
        { :title => "List of "+"Version Info".pluralize,
          :sql  => "SELECT * FROM #{@@default_query}",
          :cols => VersionInfo.value_attrs.concat(VersionInfo.foreign_key_attrs),
          :where => "version IS NOT NULL",
          :order_by => ['version']
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
    return VersionInfo.find(:all, :order => 'version')
  end


#BEGIN-UID.usermethods

#END-UID.usermethods

end
