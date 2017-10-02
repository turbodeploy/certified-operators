

require 'topology_svc'
require 'yaml'

# # #


  def get_problems
    probs = []

    svc = TopologyService.new

    mkt_uuid        = svc.get_related_uuids("MarketManager", "MainMarket").first
    act_log_uuid    = svc.get_related_uuids(mkt_uuid, "actionLog").first
    
    act_item_uuids  = svc.get_related_uuids(act_log_uuid, "items")
    xml_for_ais     = svc.xml_for(act_item_uuids, "name")

    keep_attrs = %W(explanation class_name obj_name obj_display_name obj_uuid problem_name)

    ai_hash_arr     = svc.attrs_for_each(xml_for_ais)
    for ai_hash in ai_hash_arr do
      ai_rels_xml   = svc.xml_for(ai_hash['uuid'],".*")
      ai_attrs      = svc.attrs_for(ai_rels_xml)

      explanation   = ai_attrs['explanation']
      rel_uuids     = svc.get_related_uuids(ai_hash['uuid'],"relatedProblem")
      prob_attrs    = svc.attrs_for(svc.xml_for(rel_uuids.first, "name"))

      next unless prob_attrs['displayName']
      klass, mob, prob = prob_attrs['displayName'].split("::")

      obj_attrs = svc.attrs_for(svc.xml_for(mob))

      prob_attrs['explanation']       = explanation
      prob_attrs['class_name']        = klass
      prob_attrs['obj_name']          = mob
      prob_attrs['obj_display_name']  = obj_attrs['displayName']
      prob_attrs['obj_uuid']          = obj_attrs['uuid']
      prob_attrs['problem_name']      = prob

      #puts prob_attrs.inspect

      prob_attrs.delete_if{|k,v| ! keep_attrs.include?(k)}

      probs << prob_attrs

#      rel_uuids = svc.get_related_uuids(ai_hash['uuid'],"targetServiceEntity")
#      puts svc.attrs_for(svc.xml_for(rel_uuids.first, "name"))
#
#      rel_uuids = svc.get_related_uuids(ai_hash['uuid'],"currentServiceEntity")
#      puts svc.attrs_for(svc.xml_for(rel_uuids.first, "name"))
#
#      rel_uuids = svc.get_related_uuids(ai_hash['uuid'],"newServiceEntity")
#      puts svc.attrs_for(svc.xml_for(rel_uuids.first, "name"))
      
    end

    return probs
  end


# # #

puts get_problems.to_yaml
