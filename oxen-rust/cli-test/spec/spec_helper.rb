require 'bundler/setup'
require 'aruba/rspec'
require 'fileutils'
require 'dotenv'
require 'pathname'

# TODO: look into how tests can be "grouped" together so that the
# before(:suite) and after(:suite) hooks can be used only where relevant


def run_system_command(cmd)
  puts cmd
  unless system(cmd)
    raise "Command failed: #{cmd}"
  end
end

RSpec.configure do |config|
  config.include Aruba::Api
  config.include Aruba::Matchers
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = '.rspec_status'

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:each) do

    regexp = ".env".force_encoding('UTF-16LE')
    Dotenv.load(regexp.encode('UTF-8'))
    run_system_command("oxen config --name ruby-test --email test@oxen.ai")
    system("oxen delete-remote --name ox/performance-test --host localhost:3000 -y --scheme http")
  end

  config.after(:each) do
    # Ensure the remote repository is deleted after each test
    system("oxen delete-remote --name ox/performance-test --host localhost:3000 -y --scheme http")
  end
end
