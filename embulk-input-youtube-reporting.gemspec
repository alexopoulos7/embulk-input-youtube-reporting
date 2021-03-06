
Gem::Specification.new do |spec|
  spec.name          = "embulk-input-youtube-reporting"
  spec.version       = "0.1.0"
  spec.authors       = ["Angelos Alexopoulos"]
  spec.summary       = "Youtube Reporting input plugin for Embulk"
  spec.description   = "Loads records from Youtube Reporting."
  spec.email         = ["alexopoulos7@gmail.com"]
  spec.licenses      = ["MIT"]
  spec.homepage      = "https://github.com/alexopoulos7/embulk-input-youtube-reporting"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r{^(test|spec)/})
  spec.require_paths = ["lib"]

  # TODO
  # signet 0.12.0 require >= Ruby 2.4.
  # Embulk 0.9 use JRuby 9.1.X.Y and It compatible Ruby 2.3.
  # So, Force install signet < 0.12.
  spec.add_dependency "signet", "~> 0.7", "< 0.12.0"
  spec.add_dependency "google-api-client", "~> 0.9.11"
  spec.add_development_dependency 'embulk', ['>= 0.8.38']
  spec.add_development_dependency 'bundler', ['>= 1.10.6']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
