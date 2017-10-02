class MethodsController < ApplicationController

  before_filter :login_required

  def clear_data
    
    case params[:subject]

      when "entities"
        clear_entities()
        if params[:format] == 'html'
          redirect_to :controller => 'entities', :action => 'list'
          return
        end

      when "audit_log_entries"
        clear_audit_log_entries()
        if params[:format] == 'html'
          redirect_to :controller => 'audit_log_entries', :action => 'list'
          return
        end


      when "all"
        clear_entities()
        clear_audit_log_entries()

    end

    # default response
    respond_to do |format|
      format.html {
        render :text => "<p>Response: Success</p>", :layout => "application"
      }

      format.xml {
        render :xml => {:message => "success"}.to_xml(:root => "response")
      }
    end
  end

  #---

  def clear_entities
    Entity.destroy_all("uuid not like '%Manager'")
  end

  # ---

  def clear_audit_log_entries
    AuditLogEntry.delete_all()
  end

end
