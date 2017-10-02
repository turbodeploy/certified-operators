
require 'csv'
require 'ft_modules'

class Entity < ActiveRecord::Base

   include FtUtils

   set_table_name 'entities'

   has_many             :associations, :class_name => "EntityAssn", :foreign_key => "entity_entity_id"
   has_many             :attrs, :class_name => "EntityAttr", :foreign_key => "entity_entity_id"
   has_and_belongs_to_many :member_associations, :class_name => "EntityAssn", :join_table => "entity_assns_members_entities", :association_foreign_key => "entity_assn_src_id", :foreign_key => "entity_dest_id"

   has_and_belongs_to_many :classifications, :class_name => "Classification", :join_table => "classifications_entities_entities", :association_foreign_key => "classification_src_id", :foreign_key => "entity_dest_id"



   def after_initialize()
     begin
       self.created_at = Time.now   unless self.attribute_present?('created_at')
     rescue
     end
   end

   def display_name
      return "Entity"
   end

   def self.display_name
      return "Entity"
   end

   def ctrlr_name
      return "entities"
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
      sub_items << { :title => "Associations", 
                     :items => associations.find(:all,  # :select =>"id,name",
                                   :order => 'name' ) }
      sub_items << { :title => "Attrs", 
                     :items => attrs.find(:all,  # :select =>"id,name",
                                   :order => 'name' ) }
      return sub_items
   end



    # runtime metadata support methods

    def self.attr_type_info(attr_name=nil)
        unless defined?(@@attr_info)
          @@attr_info = {}
          @@attr_info['name'] = { :datatype => 'string', :modifier => '', :length => 250, :is_identity_attr => true, :default => '', :mode => 'rw' }
          @@attr_info['display_name'] = { :datatype => 'string', :modifier => '', :length => 250, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['uuid'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['creation_class'] = { :datatype => 'string', :modifier => '', :length => 80, :is_identity_attr => false, :default => '', :mode => 'rw' }
          @@attr_info['created_at'] = { :datatype => 'DateTime'.downcase, :modifier => '', :is_identity_attr => false, :default => '', :mode => 'rw' }
        end
        return attr_name.nil? ? @@attr_info : @@attr_info[attr_name.to_s]
    end

    def self.attr_lengths
      { 'name' => 250, 'display_name' => 250, 'uuid' => 80, 'creation_class' => 80 }
    end

    def self.value_attrs
      ["name","display_name","uuid","creation_class","created_at"]
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
       ["name","display_name","uuid","creation_class"]
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


          obj.save()
        end # if identity attr supplied
      }
    end

 public


  # returns the next/prev item given its context of ownership: a) global or b) as a contained item of another object

  def next_item
    item = Entity.find(:first, :order => FtUtils.quoted_name('name'), :conditions => ["name > ? ", self.name] )
    item = Entity.find(:first, :order => FtUtils.quoted_name('name')) if item.nil? 
    return item
  end

  def prev_item
    item = Entity.find(:first, :order => FtUtils.quoted_name('name') + ' DESC', :conditions => ["name < ? ", self.name] )
    item = Entity.find(:first, :order => FtUtils.quoted_name('name') + ' DESC') if item.nil? 
    return item
  end

  def self.items_for_index
    return Entity.find(:all, :order => 'name')
  end

  # ---

  def self.default_query(cond=nil)

    return <<-EOS
          (SELECT entity.name as #{FtUtils.quoted_name('name')},
              entity.display_name as #{FtUtils.quoted_name('display_name')},
              entity.uuid as #{FtUtils.quoted_name('uuid')},
              entity.creation_class as #{FtUtils.quoted_name('creation_class')},
              entity.created_at as #{FtUtils.quoted_name('created_at')},
              entity.id as id
          FROM
              #{FtUtils.quoted_name('entities')} AS #{FtUtils.quoted_name('entity')}
          #{cond ? ("WHERE "+cond.to_s) : ""}
          ) AS entities
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
        { :title => "List of "+"Entity".pluralize,
          :sql  => "SELECT * FROM #{@@default_query}",
          :cols => Entity.value_attrs.concat(Entity.foreign_key_attrs),
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
    return Entity.find(:all, :order => 'name')
  end


#BEGIN-UID.usermethods

  def before_save
    self.display_name = self.name if self.display_name.to_s.empty?

    max_len = Entity.attr_lengths['name']
    self.name = self.name.to_s[0,max_len]

    max_len = Entity.attr_lengths['display_name']
    self.display_name = self.display_name.to_s[0,max_len]
  end

  def before_destroy
    self.associations.destroy_all
    self.attrs.destroy_all
    self.member_associations.clear
    self.classifications.clear
    entity_cache_remove(self)
  end

  def display_name
    return self.read_attribute(:display_name)
  end

  def to_xml(options = {})
    options[:indent] ||= 2
    xml = options[:builder] ||= Builder::XmlMarkup.new(:indent => options[:indent])
    xml.instruct! unless options[:skip_instruct]
    xml.entity do
      xml.name self.name
      xml.uuid self.uuid
      xml.tag! 'display-name', self.display_name
      xml.tag! 'creation-class', self.creation_class
      xml.tag! 'created-at', self.created_at.to_i*1000
      xml.classifications(:type => 'array') do
        self.classifications.each {|c| c.to_xml(:builder => xml, :skip_instruct => true)}
      end
      xml.tag! 'entity-attrs', :type => 'array' do
        self.attrs.each {|a| a.to_xml(:builder => xml, :skip_instruct => true)}
      end
      xml.tag! 'entity-assns', :type => 'array' do
        self.associations.each {|a| a.to_xml(:builder => xml, :skip_instruct => true)}
      end
    end
  end

  def get_attr_val(ft_col_name,ctx='html')
    # use the least expensive means for html contexts (lists and queries); do full eval for others (e.g. csv)
    rval = read_attribute(ft_col_name)

    unless rval
      e_attr = attrs.find(:first, :conditions => "name = '#{ft_col_name}'")
      rval = e_attr.value if e_attr
    end

    return rval
  end

  def set_attr_val(name,value)
    if Entity.value_attrs.include?(name)
      self.send("#{name}=",value)
      self.save
    else
      attr = self.attrs.find_or_create_by_name(name)
      attr.value        = value            unless value.kind_of?(Array)
      attr.multi_value  = value.join(",")  if     value.kind_of?(Array)
      attr.save
    end

    self
  end


  # # #


  LIC_DESC_NAME = "FeaturesManager"

  # class method
  def self.get_license_descriptor
    ldesc = self.find_by_name(LIC_DESC_NAME)

    ldesc = Entity.new unless ldesc
    return ldesc
  end


#END-UID.usermethods

end
