
require 'rexml/document'
require 'net/http'
require 'uri'
require 'cgi'
require 'rbconfig'
require 'api_svc'
  
class TopologyService

  ServiceLocation = "localhost"
  Port            = Config::CONFIG['host_os'].to_s["darwin"] ? 8400 : 8080
  ResourceLocation  = "http://#{ServiceLocation}:#{Port}/vmturbo"
  ResourceName      = "api/topology"
  ResourceURI       = "#{ResourceLocation}/#{ResourceName}"

  def url_for(identity,property,levels=0)
    identity = [identity].flatten.compact.collect{|ident| CGI::escape(ident)}.join("%7C")
    return "#{ResourceURI}/#{identity}?property=#{CGI::escape(property)}&levels=#{levels}"
  end
  
  def get_response(url_str)
    begin
      Net::HTTP.start(ServiceLocation, Port) {|http|
        http.read_timeout = 500
        req = Net::HTTP::Get.new(url_str)
        req.basic_auth 'guest', 'guest'
        response = http.request(req)
        resp = response.body.to_s
      }
    rescue
      puts "*** Error: Connection failed; URL = #{url_str}"
      return "<error><msg>Connection failed</msg></error>"
    end
  end

  def xml_for(identity,property="name",levels=0)
    get_request = url_for(identity,property,levels)
    xml = get_response(get_request)
    
    return xml
  end

  def get_related_uuids(identity,rel_name)
    return [] unless identity && rel_name

    xml = xml_for(identity,rel_name)
    return [] unless xml.to_s.size > 32

    uuids = []

    begin
      doc = REXML::Document.new(xml.to_s)
      root = doc.root

      for tag in root.get_elements("./TopologyElement/TopologyRelationship") do
        attrs = tag.attributes
        if(!attrs['childrenUuids'].nil?)
          uuid_arr = attrs['childrenUuids'].split("|")
          uuids = uuids + uuid_arr
        end
      end
    rescue => e
      puts e.message
      puts e.backtrace.join("\n")
      doc = root = nil
      puts "*** " + identity.to_s + " " + rel_name.to_s + " xml: " + xml.to_s
    end

    return uuids
  end

  def get_instance_uuids(class_name, rel_name="ConsistsOf")
    uuids = get_related_uuids("GROUP-#{class_name}", rel_name)
    return uuids
  end

  def attrs_for(xml,tag_name="TopologyElement")
    tag_attrs = {}

    begin
      text = xml.to_s.unpack("C*").pack("U*") # Handle ISO-8859-1 text (unicode characters > 0x80)
      doc   = REXML::Document.new(text)
      root  = doc.root

      tag       = root.get_elements("./#{tag_name}").first
      tag_attrs = tag ? tag.attributes : {}
    rescue => e
      puts e.message
      puts e.backtrace[0,5].join("\n")
      doc = root = nil
      puts xml.to_s
    end

    return tag_attrs
  end

  def attrs_for_each(xml,tag_name="TopologyElement")
    tag_attrs_arr = []

    begin
      doc = REXML::Document.new(xml.to_s)
      root = doc.root

      tags = root.get_elements("./#{tag_name}")
      for tag in tags
        tag_attrs = tag.attributes
        tag_attrs_arr << tag_attrs
      end
    rescue => e
      puts e.message
      puts e.backtrace.join("\n")
      doc = root = nil
      puts xml.to_s
    end

    return tag_attrs_arr
  end
  
  def shrink_attrs(attrs_map, keep, renames={})
    # shrunk = {}
    # keep.each do |att|
      # renamed_att = renames[att].nil? ? att : renames[att]  
      # shrunk[renamed_att] = attrs_map[att]
    # end
    shrunk = keep.inject({}) { |memo,key|
      attrs_map[key].nil? ? memo : ( memo[(renames[key].nil? ? key : renames[key])]=attrs_map[key] ; memo)
    }
    #return shrunk 
  end

end


# # # # main - test drivers # # # #

if defined?(ARGV) && ARGV.size > 0

  svc = TopologyService.new

  case

    
    when ARGV.include?("test1")

      for uuid in svc.get_instance_uuids("Storage") do

        obj_xml   = svc.xml_for(uuid)
        obj_attrs = svc.attrs_for(obj_xml)
        puts obj_attrs['creationClassName'] + " => " + obj_attrs['displayName'] + " (#{obj_attrs['uuid']})"

        for rel_uuid in svc.get_related_uuids(uuid,"Produces") do

          rel_obj_xml   = svc.xml_for(rel_uuid)
          rel_obj_attrs = svc.attrs_for(rel_obj_xml)

          puts "    " + rel_obj_attrs['creationClassName'] + " => " + rel_obj_attrs['displayName'] + " (#{rel_obj_attrs['uuid']})"
        end
      end


    when ARGV.include?("test2")

      uuids = svc.get_related_uuids("GroupManager","manages")
      for uuid in uuids do
        obj_xml   = svc.xml_for(uuid)
        obj_attrs = svc.attrs_for(obj_xml)
        puts obj_attrs.inspect
      end


    when ARGV.include?("test3")

      mkt_uuid = svc.get_related_uuids("MarketManager", "MainMarket").first
      uuids = svc.get_related_uuids(mkt_uuid, "ServiceEntities")

      for uuid in uuids do
        obj_xml   = svc.xml_for(uuid)
        obj_attrs = svc.attrs_for(obj_xml)
        puts obj_attrs.inspect
      end

    when ARGV.include?("test4")

    for uuid in svc.get_instance_uuids("LICENSE-COMMODITIES") do
      attrs = svc.attrs_for(svc.xml_for(uuid,"capacity|used|product"))
      target, product = attrs['name'].to_s, attrs['product'].to_s
      capacity, used  = attrs['capacity'].to_s, attrs['used'].to_s
      db_attrs = {
        'target' => target,
        'product' => product,
        'capacity' => capacity,
        'used' => used,
      }
      puts db_attrs.to_s
    end

  when ARGV.include?("test5")

    act_item_uuids  = svc.get_related_uuids("Market_ActionLog", "items")

    for ai_uuid in act_item_uuids do
      xml_for_ai  = svc.xml_for(ai_uuid, ".*")
      ai_hash     = svc.attrs_for(xml_for_ai)

      obj_uuid = svc.get_related_uuids(ai_hash['uuid'],"targetServiceEntity").first
      obj_attrs = svc.attrs_for(svc.xml_for(obj_uuid,"name"))

      ai_attrs = {}
      ai_attrs['obj_uuid']    = obj_attrs['uuid']
      ai_attrs['obj_class']   = obj_attrs['creationClassName']
      ai_attrs['savings']     = ai_hash['savings']
      ai_attrs['explanation'] = ai_hash['explanation']
      ai_attrs['action_type'] = ai_hash['actionType']

      puts ai_attrs.inspect
    end

    puts "There are #{act_item_uuids.size} items"
  
  when ARGV.include?("test6")
    api_svc = ApiService.new
    cmd = { :svc => "ActionLogService", :method => "getByUuid", :args => ['<selected_uuid>'] }
    puts api_svc.xml_for(cmd)
    puts "Test Retrieving rightsizing info from recommendations:"  
    action_items = []

    act_item_uuids  = svc.get_related_uuids("Market_ActionLog", "items")
  
    for ai_uuid in act_item_uuids do
      xml_for_ai = api_svc.xml_for({ :svc => "ActionLogService", :method => "getByUuid", :args => [ai_uuid] })
      # puts xml_for_ai
      ai_attrs_from_xml = svc.attrs_for(xml_for_ai,"ActionItem")
      ai_hash = svc.shrink_attrs(ai_attrs_from_xml, 
                                  ['savings','shortDescription','actionType','current','new','targetClassName'], 
                                  {'shortDescription'=>'explanation','targetClassName'=>'creationClassName'})

      if ['RESIZE'].include?(ai_hash['actionType']) #RIGHT_SIZE
        obj_attrs = svc.attrs_for(xml_for_ai,"ActionItem/Notification")
        obj_hash = svc.shrink_attrs(obj_attrs, ['notificationObjectUuid'], {'notificationObjectUuid'=>'uuid'})
        
        ai_attrs = {'obj_uuid'=>obj_hash['uuid'], 
        # 'obj_class'=>ai_hash['creationClassName'], 
        'details'=>{
          'explanation'=>ai_hash['explanation'], 
          # 'action_type'=>ai_hash['actionType'], 
          'from'=>ai_hash['current'], 
          'to'=>ai_hash['new']
        }
      }
          
        begin
          ai_attrs['details']['savings']     = ai_hash['savings'].to_f.round.to_s
        rescue
          ai_attrs['details']['savings']     = "0.00"
        end
        
        action_items << ai_attrs
        puts action_items
      end #RESIZE
      
    end #for ai_uuid
  
  when ARGV.include?("test7")
    xml = svc.xml_for("GROUP-PMsByCluster")
    attrs = svc.attrs_for(xml)
    puts attrs.inspect
  end # case
  
end
