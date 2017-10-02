ActionController::Routing::Routes.draw do |map|
  map.logout '/logout', :controller => 'sessions', :action => 'destroy'
  map.login '/login', :controller => 'sessions', :action => 'new'
#  map.register '/register', :controller => 'users', :action => 'create'
#  map.signup '/signup', :controller => 'users', :action => 'new'
  map.resources :users

  map.resource :session


  map.connect 'user_tables/get_values', :controller => 'user_tables', :action => 'get_values'
  map.connect 'user_tables/get_values.:format', :controller => 'user_tables', :action => 'get_values'

  map.connect 'entities/get_values', :controller => 'entities', :action => 'get_values'
  map.connect 'entities/get_values.:format', :controller => 'entities', :action => 'get_values'

  map.connect 'entities/delete_by_uuid', :controller => 'entities', :action => 'delete_by_uuid'
  map.connect 'entities/delete_by_uuid.:format', :controller => 'entities', :action => 'delete_by_uuid'

  map.connect 'entities/fetch_by_uuid', :controller => 'entities', :action => 'fetch_by_uuid'
  map.connect 'entities/fetch_by_uuid.:format', :controller => 'entities', :action => 'fetch_by_uuid'

  map.connect 'entities/uuid/:id', :controller => 'entities', :action => 'show'
  map.connect 'entities/uuid/:id.:format', :controller => 'entities', :action => 'show'

  map.connect ':controller/:id/related/:relname', :action => 'get_related'
  map.connect ':controller/:id/related/:relname.:format', :action => 'get_related'

  map.connect 'methods/:action/:subject', :controller => 'methods'
  map.connect 'methods/:action/:subject.:format', :controller => 'methods'

  map.connect ':controller/:action/:id', :action => /\D+/
  map.connect ':controller/:action/:id.:format', :action => /\D+/


  map.connect 'mobi/:action', :controller => 'mobi'
  map.connect 'mobi', :controller => 'mobi', :action => 'index'


  map.resources :pm_stats_by_hours
  map.resources :pm_stats_by_days
  map.resources :vm_stats_by_hours
  map.resources :vm_stats_by_days
  map.resources :ds_stats_by_hours
  map.resources :ds_stats_by_days
  map.resources :audit_log_entries
  map.resources :user_reports
  map.resources :standard_reports
  map.resources :on_demand_reports
  map.resources :report_subscriptions
  map.resources :version_infos
  map.resources :vc_licenses
  map.resources :notifications
  map.resources :entity_assns
  map.resources :classifications
  map.resources :entities
  map.resources :entity_attrs

  # The priority is based upon order of creation: first created -> highest priority.
  
  # Sample of regular route:
  # map.connect 'products/:id', :controller => 'catalog', :action => 'view'
  # Keep in mind you can assign values other than :controller and :action

  # Sample of named route:
  # map.purchase 'products/:id/purchase', :controller => 'catalog', :action => 'purchase'
  # This route can be invoked with purchase_url(:id => product.id)

# BEGIN-UID.userroutes

  map.resources :user_tables

  map.resources :entities do |entity|
    entity.resource :classifications
  end

  map.resources :classifications do |cl|
    cl.resource :entities
  end

# END-UID.userroutes

  # You can have the root of your site routed by hooking up '' 
  # -- just remember to delete public/index.html.
  # map.connect '', :controller => "welcome"
  map.connect '', :controller => 'index', :action => 'index'

  # Allow downloading Web Service WSDL as a file with an extension
  # instead of a file named 'wsdl'
  map.connect ':controller/service.wsdl', :action => 'wsdl'

  # Install the default route as the lowest priority.
  map.connect ':controller/:action/:id'
  map.connect ':controller/:action/:id.:format'

end
