class FullTextSearchController < ApplicationController

  before_filter :login_required

  layout :none

  def search
    @search_text = params[:search_text]
    redirect_to :back if ! @search_text || @search_text.size == 0

    @pattern_to_find = /#{@search_text}/i
    @results = Array.new()

    rows = PmStatsByHour.find(:all)
    rows.each { |instance|
        if instance.uuid && instance.uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'uuid' }
        end
        if instance.producer_uuid && instance.producer_uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'producer_uuid' }
        end
        if instance.property_type && instance.property_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_type' }
        end
        if instance.property_subtype && instance.property_subtype.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_subtype' }
        end
        
    }
    rows = PmStatsByDay.find(:all)
    rows.each { |instance|
        if instance.uuid && instance.uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'uuid' }
        end
        if instance.producer_uuid && instance.producer_uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'producer_uuid' }
        end
        if instance.property_type && instance.property_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_type' }
        end
        if instance.property_subtype && instance.property_subtype.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_subtype' }
        end
        
    }
    rows = VmStatsByHour.find(:all)
    rows.each { |instance|
        if instance.uuid && instance.uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'uuid' }
        end
        if instance.producer_uuid && instance.producer_uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'producer_uuid' }
        end
        if instance.property_type && instance.property_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_type' }
        end
        if instance.property_subtype && instance.property_subtype.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_subtype' }
        end
        
    }
    rows = VmStatsByDay.find(:all)
    rows.each { |instance|
        if instance.uuid && instance.uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'uuid' }
        end
        if instance.producer_uuid && instance.producer_uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'producer_uuid' }
        end
        if instance.property_type && instance.property_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_type' }
        end
        if instance.property_subtype && instance.property_subtype.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_subtype' }
        end
        
    }
    rows = DsStatsByHour.find(:all)
    rows.each { |instance|
        if instance.uuid && instance.uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'uuid' }
        end
        if instance.producer_uuid && instance.producer_uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'producer_uuid' }
        end
        if instance.property_type && instance.property_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_type' }
        end
        if instance.property_subtype && instance.property_subtype.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_subtype' }
        end
        
    }
    rows = DsStatsByDay.find(:all)
    rows.each { |instance|
        if instance.uuid && instance.uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'uuid' }
        end
        if instance.producer_uuid && instance.producer_uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'producer_uuid' }
        end
        if instance.property_type && instance.property_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_type' }
        end
        if instance.property_subtype && instance.property_subtype.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'property_subtype' }
        end
        
    }
    rows = AuditLogEntry.find(:all)
    rows.each { |instance|
        if instance.action_name && instance.action_name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'action_name' }
        end
        if instance.category && instance.category.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'category' }
        end
        if instance.user_name && instance.user_name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'user_name' }
        end
        if instance.target_object_class && instance.target_object_class.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'target_object_class' }
        end
        if instance.target_object_name && instance.target_object_name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'target_object_name' }
        end
        if instance.target_object_uuid && instance.target_object_uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'target_object_uuid' }
        end
        if instance.source_class && instance.source_class.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'source_class' }
        end
        if instance.source_name && instance.source_name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'source_name' }
        end
        if instance.source_uuid && instance.source_uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'source_uuid' }
        end
        if instance.destination_class && instance.destination_class.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'destination_class' }
        end
        if instance.destination_name && instance.destination_name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'destination_name' }
        end
        if instance.destination_uuid && instance.destination_uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'destination_uuid' }
        end
        if instance.details && instance.details.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'details' }
        end
        
    }
    rows = UserReport.find(:all)
    rows.each { |instance|
        if instance.title && instance.title.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'title' }
        end
        if instance.category && instance.category.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'category' }
        end
        if instance.short_desc && instance.short_desc.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'short_desc' }
        end
        if instance.description && instance.description.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'description' }
        end
        if instance.xml_descriptor && instance.xml_descriptor.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'xml_descriptor' }
        end
        if instance.period && instance.period.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'period' }
        end
        if instance.day_type && instance.day_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'day_type' }
        end
        
    }
    rows = StandardReport.find(:all)
    rows.each { |instance|
        if instance.filename && instance.filename.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'filename' }
        end
        if instance.title && instance.title.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'title' }
        end
        if instance.category && instance.category.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'category' }
        end
        if instance.short_desc && instance.short_desc.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'short_desc' }
        end
        if instance.description && instance.description.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'description' }
        end
        if instance.period && instance.period.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'period' }
        end
        if instance.day_type && instance.day_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'day_type' }
        end
        
    }
    rows = OnDemandReport.find(:all)
    rows.each { |instance|
        if instance.filename && instance.filename.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'filename' }
        end
        if instance.title && instance.title.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'title' }
        end
        if instance.category && instance.category.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'category' }
        end
        if instance.short_desc && instance.short_desc.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'short_desc' }
        end
        if instance.description && instance.description.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'description' }
        end
        if instance.obj_type && instance.obj_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'obj_type' }
        end
        if instance.scope_type && instance.scope_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'scope_type' }
        end
        
    }
    rows = ReportSubscription.find(:all)
    rows.each { |instance|
        if instance.email && instance.email.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'email' }
        end
        if instance.period && instance.period.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'period' }
        end
        if instance.day_type && instance.day_type.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'day_type' }
        end
        if instance.obj_uuid && instance.obj_uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'obj_uuid' }
        end
        
    }
    rows = VersionInfo.find(:all)
    rows.each { |instance|
        
    }
    rows = VcLicense.find(:all)
    rows.each { |instance|
        if instance.target && instance.target.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'target' }
        end
        if instance.product && instance.product.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'product' }
        end
        
    }
    rows = Notification.find(:all)
    rows.each { |instance|
        if instance.severity && instance.severity.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'severity' }
        end
        if instance.category && instance.category.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'category' }
        end
        if instance.name && instance.name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'name' }
        end
        if instance.uuid && instance.uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'uuid' }
        end
        if instance.description && instance.description.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'description' }
        end
        
    }
    rows = EntityAssn.find(:all)
    rows.each { |instance|
        if instance.name && instance.name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'name' }
        end
        
    }
    rows = Classification.find(:all)
    rows.each { |instance|
        if instance.name && instance.name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'name' }
        end
        
    }
    rows = Entity.find(:all)
    rows.each { |instance|
        if instance.name && instance.name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'name' }
        end
        if instance.display_name && instance.display_name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'display_name' }
        end
        if instance.uuid && instance.uuid.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'uuid' }
        end
        if instance.creation_class && instance.creation_class.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'creation_class' }
        end
        
    }
    rows = EntityAttr.find(:all)
    rows.each { |instance|
        if instance.name && instance.name.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'name' }
        end
        if instance.value && instance.value.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'value' }
        end
        if instance.multi_values && instance.multi_values.to_s.index(@pattern_to_find)
            @results << { :obj => instance, :field => 'multi_values' }
        end
        
    }

  end


#BEGIN-UID.usermethods

  def search
    # this method is not appropriate for this appliacation
    @results = []
  end

#END-UID.usermethods


end
