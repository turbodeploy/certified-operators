class IndexController < ApplicationController

  before_filter :login_required

  def index
    @ft_subject = "VMT DB"
    @ft_activity  = "Contents"
    render :action => @ft_custom_action if @ft_custom_action
  end

#BEGIN-UID.usermethods

#END-UID.usermethods


end
