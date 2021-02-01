module Fog
  module Compute
    class Google
      class Mock
        def delete_target_instance(_target_name, _zone)
          # :no-coverage:
          Fog::Mock.not_implemented
          # :no-coverage:
        end
      end

      class Real
        def delete_target_instance(target_name, zone)
          zone = zone.split("/")[-1] if zone.start_with? "http"
          @compute.delete_target_instance(@project, zone, target_name)
        end
      end
    end
  end
end
