module Fog
  module Storage
    class GoogleJSON
      class Real
        # Create an object in an Google Storage bucket
        # https://cloud.google.com/storage/docs/json_api/v1/objects/insert
        #
        # @param bucket_name [String] Name of bucket to create object in
        # @param object_name [String] Name of object to create
        # @param data [File|String|Paperclip::AbstractAdapter] File, String or Paperclip adapter to create object from
        # @option predefined_acl [String]  Applies a predefined set of access controls to this bucket.
        # @option options [String] "cache_control" Caching behaviour
        # @option options [DateTime] "content_disposition" Presentational information for the object
        # @option options [String] "content_encoding" Encoding of object data
        # @option options [String] "md5_hash" Base64 encoded 128-bit MD5 digest of message (defaults to Base64 encoded MD5 of object.read)
        # @option options [String] "content_type" Standard MIME type describing contents (defaults to MIME::Types.of.first)ols
        # @option options [Array] "acl" ACL list
        # @option options [Hash] "metadata" User metadata for the object
        # @option options [String] "storage_class" Storage class of the object
        # @return [Google::Apis::StorageV1::Object]
        def put_object(bucket_name, object_name, data, predefined_acl, options = {})
          data, detected_type = data_source_and_type(data)
          options["content_type"] ||= detected_type
          options["name"] = object_name

          object_config = ::Google::Apis::StorageV1::Object.new(options.symbolize_keys)
          @storage_json.insert_object(bucket_name,
                                      object_config,
                                      :upload_source => data,
                                      :predefined_acl => predefined_acl,
                                      :content_type => options['content_type'],
                                      :content_encoding => options['content_encoding'],
                                      :options => ::Google::Apis::RequestOptions.default)
        end

        def data_source_and_type(data)
          if data.is_a?(String)
            [StringIO.new(data), "text/plain"]
          elsif data.is_a?(::File)
            [data, Fog::Storage.parse_data(data)[:headers]["Content-Type"]]
          elsif data.respond_to?(:content_type) && data.respond_to?(:path)
            [data.path, data.content_type]
          end
        end

      end

      class Mock
        def put_object(_bucket_name, _object_name, _data, _options = {})
          raise Fog::Errors::MockNotImplemented
        end
      end
    end
  end
end
