
  def in_place_form_methods_for(object, properties=object.to_s.camelize.constantize.value_attrs, options = {})
    
    define_method("w20_show_#{object}") do
      @properties = (params[:props] && params[:props].split("/")) || properties
      @obj = object.to_s.camelize.constantize.find(params[:id])
      render :inline => "<p>No #{object.to_s.camelize} with id '#{params[:id].to_s}'</p>" and return if ! @obj
      render :partial => "fast_ops/w20_show_object"
    end

    define_method("w20_edit_#{object}") do
      @properties = (params[:props] && params[:props].split("/")) || properties
      @obj = object.to_s.camelize.constantize.find(params[:id])
      render :inline => "<p>No #{object.to_s.camelize} with id '#{params[:id].to_s}'</p>" and return if ! @obj
      render :partial => "fast_ops/w20_edit_object"
    end

    define_method("w20_update_#{object}") do
      @properties = (params[:props] && params[:props].split("/")) || properties
      @obj = object.to_s.camelize.constantize.find(params[:id]) if params[:id]
      render :inline => "<p>No #{object.to_s.camelize} with id '#{params[:id].to_s}'</p>" and return if ! @obj
      if params[:obj]
        @obj.update_attributes params[:obj]
      end
      render :partial => "fast_ops/w20_show_object"
    end

  end

#BEGIN-UID.usermethods

#END-UID.usermethods

