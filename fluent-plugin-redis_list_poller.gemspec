# coding: utf-8

Gem::Specification.new do |spec|
  spec.name          = "fluent-plugin-redis_list_poller"
  spec.version       = "1.0.0"
  spec.authors       = ["Jonathan Serafini"]
  spec.email         = ["jonathan@serafini.ca"]

  spec.summary       = %q{A fluentd redis input plugin supporting batch operations}
  spec.description   = spec.summary
  spec.homepage      = "https://github.com/JonathanSerafini/fluent-plugin-redis_list_poller"
  spec.license       = "apache2"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.executables   = []
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake"

  spec.add_runtime_dependency "redis", [">= 3.3.0", "< 3.4.0"]
  spec.add_runtime_dependency "fluentd", [">= 0.14.0", "< 2"]
end
