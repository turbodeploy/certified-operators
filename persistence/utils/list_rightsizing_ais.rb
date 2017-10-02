

require 'topology_svc'
require 'yaml'

# # #

  def get_rightsizing_actions
    action_items = []

    svc = TopologyService.new

    act_item_uuids  = svc.get_related_uuids("RightsizingActionLog", "items")
    xml_for_ais     = svc.xml_for(act_item_uuids, ".*")

    ai_hash_arr     = svc.attrs_for_each(xml_for_ais)
    for ai_hash in ai_hash_arr do
      obj_uuid = svc.get_related_uuids(ai_hash['uuid'],"targetServiceEntity").first
      obj_attrs = svc.attrs_for(svc.xml_for(obj_uuid,"name"))

      ai_attrs = {}
      ai_attrs['obj_uuid']    = obj_attrs['uuid']
      ai_attrs['obj_class']   = obj_attrs['creationClassName']
      ai_attrs['explanation'] = ai_hash['explanation']
      ai_attrs['action_type'] = ai_hash['actionType']

      action_items << ai_attrs
    end

    return action_items
  end


# # #

ais = get_rightsizing_actions()

puts ais.to_yaml
