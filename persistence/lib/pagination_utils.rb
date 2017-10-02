
module PaginationUtils

    # Paginates an existing AR result set, returning the Paginator and collection slice.
    #
    # Based upon:
    # http://www.bigbold.com/snippets/posts/show/389
    #
    # Options:
    # +:collection+: the collection to paginate
    # +:per_page+: records per page
    # +:page+: page
    #
    # Example:
    #   complex_query_result = Customer.find_by_sql('something complex')
    #   @pages, @customers = paginate_collection(:collection => complex_query_result)
    # 
    # Alternatively, you can specify a block, the result of which will be used as the collection:
    #   @pages, @customers = paginate_collection { Customer.find_by_sql('something complex') }
    #

    def paginate_collection(options = {}, &block)
      if block_given?
        options[:collection] = block.call
      elsif !options.include?(:collection)
        raise ArgumentError, 'You must pass a collection in the options or using a block'
      end
      
      default_options = {:per_page => 10, :page => 1}
      options = default_options.merge options

      pages = ActionController::Pagination::Paginator.new self, options[:collection].size, options[:per_page], options[:page]

      first = pages.current.offset

      last = [first + options[:per_page], options[:collection].size].min
      slice = options[:collection][first...last]
      return [pages, slice]
    end

#BEGIN-UID.usermethods

#END-UID.usermethods

end

