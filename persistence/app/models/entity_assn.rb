
require 'csv'
require 'ft_modules'

class EntityAssn < ActiveRecord::Base

   include FtUtils

   set_table_name 'entity_assns'

   has_and_belongs_to_many :members, :class_name => "Entity", :join_table => "entity_assns_members_entities", :foreign_key => "entity_assn_src_id", :association_foreign_key => "entity_dest_id"

   belongs_to           :entity, :class_name => "Entity", :foreign_key => "entity_entity_id"


   def after_initialize()
     begin
     rescue
     end
   end

   def display_name
      return "Entity Assn"
   end

   def self.display_name
      return "Entity Assn"
   end

   def ctrlr_name
      return "entity_assns"
   end

   def self.iattr_name
      return "name"
   end

   def iattr_name
      return "name"
   end

   def iname
      return "" if self.name.nil? 
      return self.name.to_s
   end

   def qname
      q_name = iname()
      q_name = entity.qname() + " > " + q_name if entity
      return q_name
   end

   def sname
       return self.qname
   end

   def container_qname
      return (entity && entity.qname) || ""
   end

   def container_obj
      return nil || entity
   end

  def move_targets()
    targets = Entity.find(:all)
    targets = targets - [self.container_obj()] if self.container_obj()
    return targets
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
          @@attr_info['name'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => true, :default => '', :mode => 'rw' }
        end
        return attr_name.nil? ? @@attr_info : @@attr_info[attr_name.to_s]
    end

    def self.attr_lengths
      { 'name' => 80 }
    end

    def self.value_attrs
      ["name"]
    end

    def self.foreign_key_attrs
       ["entity"]
    end

    def self.foreign_key_attr_info(fk_attr_name=nil)
      unless defined?(@@fk_attr_info)
        @@fk_attr_info = { }
        @@fk_attr_info['entity'] = {:datatype => "Entity", :foreign_key => "entity_entity_id"}
      end

      return fk_attr_name.nil? ? @@fk_attr_info : @@fk_attr_info[fk_attr_name.to_s]
    end

    def self.get_form_fields()
       ["name"]
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
        unless row[:name].to_s.empty? then
          obj = find_or_create_by_name(row[:name])

          attrs = row.clone
          attrs.delete_if { |k,v| (! self.value_attrs.member? k.to_s) || v.size == 0 }
          obj.attributes = attrs

          obj.entity = Entity.find_or_create_by_name(row[:entity]) unless row[:entity].to_s.empty?

          obj.save()
        end # if identity attr supplied
      }
    end

 public


  # returns the next/prev item given its context of ownership: a) global or b) as a contained item of another object

  def next_at_top
    item = EntityAssn.find(:first, :order => FtUtils.quoted_name('name'), :conditions => ["name > ? AND entity_entity_id is null", self.name] )
    item = EntityAssn.find(:first, :order => FtUtils.quoted_name('name'), :conditions => "entity_entity_id is null") if item.nil? 
    return item
  end

  def prev_at_top
    item = EntityAssn.find(:first, :order => FtUtils.quoted_name('name') + ' DESC', :conditions => ["name < ? AND entity_entity_id is null", self.name] )
    item = EntityAssn.find(:first, :order => FtUtils.quoted_name('name') + ' DESC', :conditions => "entity_entity_id is null") if item.nil? 
    return item
  end

  def next_item
    return next_at_top() unless self.container_obj()
    item = self.container_obj().associations.find(:first, :order => FtUtils.quoted_name('name'), :conditions => ["name > ? ", self.name] )
    item = self.container_obj().associations.find(:first, :order => FtUtils.quoted_name('name')) if item.nil? 
    return item
  end

  def prev_item
    return prev_at_top() unless self.container_obj()
    item = self.container_obj().associations.find(:first, :order => FtUtils.quoted_name('name') + ' DESC', :conditions => ["name < ? ", self.name] )
    item = self.container_obj().associations.find(:first, :order => FtUtils.quoted_name('name') + ' DESC') if item.nil? 
    return item
  end

  def self.items_for_index
    return EntityAssn.find(:all, :order => 'name')
  end

  # ---

  def self.default_query(cond=nil)

    return <<-EOS
          (SELECT entity_assn.name as #{FtUtils.quoted_name('name')},
              entity.name as #{FtUtils.quoted_name('entity')},
              entity.id as entity_entity_id,
              entity_assn.id as id
          FROM
              #{FtUtils.quoted_name('entity_assns')} AS #{FtUtils.quoted_name('entity_assn')}
              left outer join #{FtUtils.quoted_name('entities')} as #{FtUtils.quoted_name('entity')} on entity_assn.entity_entity_id = entity.id
          #{cond ? ("WHERE "+cond.to_s) : ""}
          ) AS entity_assns
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
        { :title => "List of "+"Entity Assn".pluralize,
          :sql  => "SELECT * FROM #{@@default_query}",
          :cols => EntityAssn.value_attrs.concat(EntityAssn.foreign_key_attrs),
          :where => "name IS NOT NULL",
          :order_by => ['name']
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
    return EntityAssn.find(:all, :order => 'name')
  end


#BEGIN-UID.usermethods

  def to_xml(options = {})
    options[:indent] ||= 2
    xml = options[:builder] ||= Builder::XmlMarkup.new(:indent => options[:indent])
    xml.instruct! unless options[:skip_instruct]
    xml.tag! 'entity-assn' do
      xml.name self.name
      xml.members(:type => 'array') do
        self.members.each{ |e|
          xml.member do
            xml.uuid e.uuid
          end
        }
      end
    end
  end

  def before_save
    entity_cache_remove(self.entity) if self.entity
  end


#END-UID.usermethods

end
