require 'api_svc'
$api_svc = ApiService.new

module Authentication
  module ByPassword
    # Stuff directives into including module
    def self.included(recipient)
      recipient.extend(ModelClassMethods)
      recipient.class_eval do
        include ModelInstanceMethods
        
        # Virtual attribute for the unencrypted password
        attr_accessor :password
        validates_presence_of     :password,                   :if => :password_required?
        validates_presence_of     :password_confirmation,      :if => :password_required?
        validates_confirmation_of :password,                   :if => :password_required?
        validates_length_of       :password, :within => 6..40, :if => :password_required?
        before_save :encrypt_password
      end
    end # #included directives

    #
    # Class Methods
    #
    module ModelClassMethods
      # This provides a modest increased defense against a dictionary attack if
      # your db were ever compromised, but will invalidate existing passwords.
      # See the README and the file config/initializers/site_keys.rb
      #
      # It may not be obvious, but if you set REST_AUTH_SITE_KEY to nil and
      # REST_AUTH_DIGEST_STRETCHES to 1 you'll have backwards compatibility with
      # older versions of restful-authentication.
      def password_digest(password, salt)
        digest = REST_AUTH_SITE_KEY
        REST_AUTH_DIGEST_STRETCHES.times do
          digest = secure_digest(digest, salt, password, REST_AUTH_SITE_KEY)
        end
        digest
      end      
    end # class methods

    #
    # Instance Methods
    #
    module ModelInstanceMethods
      
      # Encrypts the password with the user salt
      def encrypt(password)
        self.class.password_digest(password, salt)
      end
      
      # http://guest:guest@localhost:8400/vmturbo/api?inv.c&LoginService&authenticate&login&password
      def authenticated?(password)
        #crypted_password == encrypt(password)
        
    		cmd = { :svc => "LoginService",
               :method => "authenticate",
               :args => [login.to_s,password.to_s]
             }
    		response = $api_svc.xml_for(cmd)
    		#puts "Authentication response: "+response
    		#puts response.inspect
    		return (response==true || response.strip=="true")
      end
      
      # before filter 
      def encrypt_password
        return if password.blank?
        self.salt = '' #self.class.make_token if new_record? ARIEL
        self.crypted_password = encrypt(password)
      end
      def password_required?
        crypted_password.blank? || !password.blank?
      end
    end # instance methods
  end
end
