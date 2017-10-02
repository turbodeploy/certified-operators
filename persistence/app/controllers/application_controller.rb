# Filters added to this controller will be run for all controllers in the application.
# Likewise, all the methods added will be available for all controllers.

require 'pp'
require 'ft_controller_macros'
require 'ft_modules'

class ApplicationController < ActionController::Base
   include AuthenticatedSystem
   
   # Pick a unique cookie name to distinguish our session data from others'
   # session :session_key => '_persistence'

   include FtUtils
   include ApplicationHelper
   
   unless ActionController::Base.consider_all_requests_local
    rescue_from Exception, :with => :render_error
  end 

   # caches_action :show, :show_as_doc, :list, :block_list
   # before_filter :invalidate_caches, :except => [ :show, :show_as_doc, :list, :block_list, :new, :index ]

   def invalidate_caches
      expire_action :controller => 'index', :action => 'index'
      expire_action :controller => controller_name, :action => 'list'
      expire_action :controller => controller_name, :action => 'block_list'
      expire_action :controller => controller_name, :action => 'show', :id => params[:id] if params[:id] 

      expire_fragment %r{.*list.*} if params[:action] == "toggle_list_text_toggle"

      if params.keys.select{|kv| kv.include?("_id")}.size > 0 || params[:action] == 'update' || params[:action] == 'destroy' || params[:action].include?("delete") then
        expire_fragment %r{.*show.*} 
        expire_fragment %r{.*list.*} 
      end

      return true
   end

   before_filter :fix_req
   def fix_req
     params[:format] = "html" if ! params[:format]
     return true
   end

  public 

  # prevents method def exceptions - does nothing and returns nil
  def method_missing(methName) end

  def get_order_field params, obj_name, coll_name, default_order
     if params[:order_sublist] && params[:order_by] then
      session["order_sublist_"+obj_name+"_"+params[:order_sublist]] = params[:order_by]
     end
      
     return session["order_sublist_"+obj_name+"_"+coll_name] if session["order_sublist_"+obj_name+"_"+coll_name] && session["order_sublist_"+obj_name+"_"+coll_name].length > 0
     return default_order
  end

  before_filter :customs, :only => [ :index, :new, :edit, :show, :show_as_doc, :list, :block_list, 
                                     :login, :register, :edit_reg_info, :change_password, :user_main_view ]
  def customs
    @ft_custom_action = nil
    return true if session['user'] && session['user'].login == 'admin'

    if File.exist?("#{RAILS_ROOT}/app/views/#{controller_name}/#{params[:action]}_custom.html.erb")
      @ft_custom_action = "#{params[:action]}_custom"
    end

    return true
  end

#BEGIN-UID.usermethods

  before_filter :set_pwd
  def set_pwd
    if params[:pwd] && params[:pwd] == "VMTurbo" then
      session[:auth] = true
    end
    return true
  end
  
  def render_error(exception)
    log_error(exception)
    @cur_exception = exception
    render :template => "/error/500.html.erb", :status => 500
  end
  
  def url_for(options)
    case options
    when Hash
      options.merge!({:only_path=>true})
    end
    new_url = super(options)
    #puts new_url
    return new_url 
  end

#END-UID.usermethods

end
