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

class EntitySplitter

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
    cache_dir = file_path.gsub("doc_queue","doc_cache").gsub(/(\\|\/)[^\\\/]+$/,'')
    FileUtils.mkdir cache_dir, :mode => 0777 unless File.exist?(cache_dir)

    xml_doc = load_doc(file_path)

    if xml_doc
      root = xml_doc.root

      entities = xpath_get(root,"//entity")
      for entity_xml in entities do
        make_entity_cache_file(entity_xml, cache_dir, quiet)
      end
    end

    entities = nil
    xml_doc = nil
  end

  # ---

  def make_entity_cache_file(entity_xml, cache_dir, quiet)
    entity_attrs = entity_attrs_from(entity_xml)

    file_path = cache_dir+"/"+entity_attrs[:uuid]+".xml"
    puts entity_attrs[:name]+" -> "+file_path unless quiet

    File.open(file_path, "w") do |f|
      f.write(entity_xml.to_s)
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

      when "creation-class"
        entity_attrs[:creation_class] = child.content

      end
    end

    return entity_attrs
  end

end

# ------------------------------------------------------------

class EntitySplitterNokogiri < EntitySplitter

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

  def make_entity_cache_file(entity_xml, cache_dir, quiet)
    entity_attrs = entity_attrs_from(entity_xml)

    file_path = cache_dir+"/"+entity_attrs[:uuid]+".xml"
    puts entity_attrs[:name]+" -> "+file_path unless quiet

    File.open(file_path, "w") do |f|
      f.write('<?xml version="1.0"?>'+"\n"+entity_xml.to_xml)
    end
  end

end


def process_one(filename,quiet)
  return unless File.exists?(filename)

  puts "Processing " + filename unless quiet

  t1 = Time.now

  if defined?(Nokogiri)
    loader = EntitySplitterNokogiri.new
  else
    loader = EntitySplitter.new
  end

  loader.process_file(filename, quiet)

  t2 = Time.now

  puts " done " + (t2-t1).to_s + " seconds." unless quiet
end


# ---

args = Hash[*ARGV]

quiet = args["-q"].to_s != "false"
filepath = args["-f"]

if filepath
  process_one(filepath,quiet)
else
  puts "missing -f arg (the xml file to split)"
end

