class PublicPagesController < ApplicationController

  before_filter :login_required

    # this file is only generated if it does not already exist - you may safely change its contents

    def index
        redirect_to :action => 'main'
    end

    def main
        @ft_subject = "Public Interface"
        @ft_activity = "Main Page"
    end

end

