
require 'rubygems'
require 'fileutils'

require 'rbconfig'

begin
  if Config::CONFIG['host_os'].to_s[/mingw|mswin/]
    require 'nokogiri'
  else
    require 'libxml'
  end
rescue
end

# -----------------------------------------------------------

class EntityLoader

  def load_doc(file_path)
    fh = File.open(file_path, "r")

    begin
      xml_parser = LibXML::XML::Parser.io(fh, :options => LibXML::XML::Parser::Options::NOENT |
                                                          LibXML::XML::Parser::Options::NOBLANKS )
      xml_doc = xml_parser.parse
    rescue
      xml_doc = nil
      puts "Error during XML parsing of file " + file_path.to_s
    end

    fh.close
    
    xml_parser = nil

    return xml_doc
  end

  def xpath_get(xml_node,xpath_expr)
    return [] unless xml_node
    return xml_node.find(xpath_expr).to_a
  end

  def process_file(file_path, quiet)
    # preload classifications
    @name_to_classification = { }
    @classifications = Classification.find(:all)
    for @cl in @classifications do
      @name_to_classification[@cl.name] = @cl
    end

    cache_dir = file_path.gsub("doc_queue","doc_cache").gsub(/(\\|\/)[^\\\/]+$/,'')
    FileUtils.mkdir cache_dir, :mode => 0777 unless File.exists?(cache_dir)

    xml_doc = load_doc(file_path)

    if xml_doc
      root = xml_doc.root

      entities = xpath_get(root,"//entity")

      # establish the entities existence in the db
      for entity_xml in entities do
        FileUtils.touch $semaphore_file

        process_entity(entity_xml, quiet)
      end
    end
    
    entities = nil
    xml_doc = nil
  end

  # ---

  META_GROUPS = [ 
    "GROUP-AllVirtualMachine",
    "GROUP-Application",
    "GROUP-DataCenter",
    "GROUP-MyGroups",
    "GROUP-OtherGroups",
    "GROUP-PhysicalMachine",
    "GROUP-PMGroups",
    "GROUP-Storage",
    "GROUP-VirtualMachine",
    "GROUP-VMGroups",
    "GROUP-PhysicalMachineByCluster",
    "GROUP-PhysicalMachineByDataCenter",
    "GROUP-PM_Parent",
    "GROUP-VirtualMachineByCluster",
    "GROUP-VirtualMachineByDataCenter",
    "GROUP-VirtualMachineByFolder",
    "GROUP-VirtualMachineByNetwork",
    "GROUP-VirtualMachineByPhysicalMachine",
    "GROUP-VirtualMachineByStorage",
    "GROUP-VCImportedGroups",
  ]
  
  
  def process_entity(entity_xml, quiet)
    overwrite    = true # assume always overwrite # (entity_xml.attributes[:overwrite] || "true") == "true"
    entity_attrs = entity_attrs_from(entity_xml)

    puts entity_attrs[:name]+" -> DB" unless quiet


    #TODO - make queries use the Group classification instead of creation class
    entity_attrs[:creation_class] = "Group" if entity_attrs[:creation_class] == "Folder"


    # UUIDs for groups are not durable.
    # Get rid of old groups having this name but differing UUIDs

    if ["Group","MetaGroup"].include?(entity_attrs[:creation_class].to_s)
      Entity.destroy_all("name = '#{entity_attrs[:name]}' AND creation_class in ('Group','MetaGroup') and uuid <> '#{entity_attrs[:uuid]}'")
      puts "  Deleted redundant group entity #{entity_attrs[:name]}" unless quiet
    end

    if META_GROUPS.include?(entity_attrs[:name]) || 
         entity_attrs[:name].to_s.include?(", ") then

      entity_attrs[:creation_class] = "MetaGroup"

    end


    unless @entity = Entity.find(:first, :conditions => "uuid = '#{entity_attrs[:uuid]}'")
      @entity = Entity.new
    end
    
    @entity.attributes = entity_attrs
    @entity.save()

    create_or_update_nested_items(entity_xml,entity_attrs,overwrite)
  end

  # ---

  def create_or_update_nested_items(entity_xml,entity_attrs,overwrite)

    
    classification_names = xpath_get(entity_xml,"./classifications/classification/name").collect{|cname| cname.content}
    @entity.classifications.clear if overwrite && classification_names.size > 0

    c_ids = @entity.classification_ids

    for cl_name in classification_names do
      ccl = defined?(@name_to_classification) ? @name_to_classification[cl_name] : nil
      ccl = Classification.find_or_create_by_name(cl_name) unless ccl
      unless c_ids.include?(ccl.id)
        @entity.classifications << ccl
        c_ids << ccl.id
      end
    end

    unless entity_attrs[:creation_class].to_s.empty?
      ccl = defined?(@name_to_classification) ? @name_to_classification[entity_attrs[:creation_class]] : nil
      ccl = Classification.find_or_create_by_name(entity_attrs[:creation_class]) unless ccl
      unless c_ids.include?(ccl.id)
        @entity.classifications << ccl
        c_ids << ccl.id
      end
    end


    attrs_xml_arr = xpath_get(entity_xml,"./entity-attrs/entity-attr")
    @entity.attrs.destroy_all if overwrite && attrs_xml_arr.size > 0

    for attr_xml in attrs_xml_arr do
      attribute_attrs = attribute_attrs_from(attr_xml)

      next if attribute_attrs[:value].to_s.empty?

      if attribute_attrs[:value].instance_of?(Array) then
        attribute_attrs[:multi_values] = attribute_attrs[:value].join(",")
        attribute_attrs[:value] = nil
      elsif attribute_attrs[:value].to_s.include?(",")
        attribute_attrs[:multi_values] = attribute_attrs[:value].to_s
        attribute_attrs[:value] = nil
      else
        attribute_attrs[:multi_values] = nil
      end

      if attr_obj = @entity.attrs.find(:first, :conditions => "name = '#{attribute_attrs[:name]}'")
        attr_obj.update_attributes(attribute_attrs)
      else
        @entity.attrs.create(attribute_attrs)
      end
    end


    assns_xml_arr = xpath_get(entity_xml,"./entity-assns/entity-assn")
    @entity.associations.destroy_all if overwrite && assns_xml_arr.size > 0

    for assn_xml in assns_xml_arr do
      name = xpath_get(assn_xml,"./name").first.content
      unless assn_obj = @entity.associations.find(:first, :conditions => "name = '#{name}'")
        assn_obj = @entity.associations.create(:name => name)
      end

      m_ids = assn_obj.member_ids
      member_uuids = xpath_get(assn_xml,"./members/member/uuid").collect{|m| m.content}
      for member_uuid in member_uuids do
        # create the member if it is not there
        unless ref_obj = Entity.find(:first, :conditions => "uuid = '#{member_uuid}'")
          ref_obj = Entity.create(:name => member_uuid, :uuid => member_uuid)
        end
        assn_obj.members << ref_obj unless m_ids.include?(ref_obj.id)
      end
    end

  end

  # ---

  def entity_attrs_from(entity_xml)
    entity_attrs = { }

    for child in entity_xml.children do
      case child.name

      when "uuid"
        entity_attrs[:uuid] = child.content

      when "name"
        entity_attrs[:name] = child.content

      when "display-name"
        entity_attrs[:display_name] = child.content

      when "creation-class"
        entity_attrs[:creation_class] = child.content

      end
    end

    entity_attrs[:display_name] = entity_attrs[:name] if entity_attrs[:display_name].to_s.empty?

    return entity_attrs
  end

  # ---

  def attribute_attrs_from(attr_xml)
    attrs = { }

    for child in attr_xml.children do
      case child.name

      when "name"
        attrs[:name] = child.content

      end
    end

    # an attribute can have multiple values
    value = xpath_get(attr_xml,"./value").collect{|v| v.content}
    if value.size == 0
      value = ""
    elsif value.size == 1
      value = value[0]
    end

    attrs[:value] = value

    return attrs
  end

end

# ------------------------------------------------------------

class EntityLoaderNokogiri < EntityLoader

  def load_doc(file_path)
    begin
      xml_doc = Nokogiri::XML.parse(File.read(file_path))
    rescue
      xml_doc = nil
      puts "Error during XML parsing of file " + file_path.to_s
    end

    return xml_doc
  end

  def xpath_get(xml_node,xpath_expr)
    return [] unless xml_node
    return  [xml_node.search(xpath_expr)].flatten
  end
end

# ------------------------------------------------------------

def process_one(filename,quiet)
  begin
    return unless File.exists?(filename)

    puts "Processing " + filename unless quiet

    t1 = Time.now

    if defined?(Nokogiri)
      loader = EntityLoaderNokogiri.new
    else
      loader = EntityLoader.new
    end

    loader.process_file(filename, quiet)

    t2 = Time.now

    puts " done " + (t2-t1).to_s + " seconds." unless quiet

    new_name = filename+".success."+(Time.now.to_f*1000).to_i.to_s
    File.rename(filename, new_name) if File.exists?(filename)
    puts "file " + filename + " renamed to " + new_name unless quiet

  rescue
    new_name = filename+".failed."+(Time.now.to_f*1000).to_i.to_s
    File.rename(filename, new_name) if File.exists?(filename)
    puts "file " + filename + " renamed to " + new_name unless quiet
    
    raise unless quiet
  end
end

# ---

def process_all(dir_path, quiet)
  $count = 0

  # process the in-coming files in the order they arrived by their names
  Dir.new(dir_path).select{|d| d[/\.xml$/]}.sort.each { |filename|
    filename = dir_path+"/"+filename
    process_one(filename,quiet)
    
    $count += 1
  }

  puts "Processed #{$count} files." unless quiet
end

# ---

def back_off?
  if File.exists?($semaphore_file)
    if (Time.now-File.mtime($semaphore_file) > 600) then
      # file has not been touched in over 10 mins; likely a 
      # killed process had it - reclaim it and continue

      File.open($semaphore_file,"w"){|f| f.write($my_id_number)}
      return false
    end

    sem_id_number = File.read($semaphore_file)
    
    if sem_id_number.empty? || sem_id_number == "semaphore" then
      File.open($semaphore_file,"w"){|f| f.write($my_id_number)}
      return false
    elsif sem_id_number == $my_id_number then
      return false
    else
      return true   # another loader process has it
    end

  else
    FileUtils.mkdir_p "/tmp" 
    File.open($semaphore_file,"w"){|f| f.write($my_id_number)}
    return false

  end
end

# --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- --- ---

$semaphore_file = "/tmp/vmt_entity_loader.sem"
$my_id_number   = (Time.now.to_f*1000).to_i.to_s   # current time in MS

exit if back_off?

args     = Hash[*ARGV]
quiet    = args["-q"].to_s != "false"
filepath = args["-f"]   # currently ignored

begin
  dir_path = ENV["persistence_BASE"] ? ENV["persistence_BASE"]+"/doc_queue" : "./doc_queue"

  $count = 1
  while $count > 0 do
    process_all(dir_path, quiet)
  end
rescue
  # trap everything - we want to ensure that we clean up properly
end

File.open($semaphore_file,"w"){|f| f.write("")}
FileUtils.rm(Dir.glob(dir_path+"/*success*"))
