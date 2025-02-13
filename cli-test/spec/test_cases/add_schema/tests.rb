require 'spec_helper'
require 'json'
require 'shellwords'
require 'open3'

RSpec.describe 'schemas add - test relative paths', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    FileUtils.rm_rf('test-schema-paths')

  end

  it 'tests oxen schemas add with relative paths from subdirectories' do
  
    # Setup base repo
    directory_path = File.join('tmp', 'aruba', 'test-relative-dirs')
    FileUtils.mkdir_p(directory_path)

    # Capitalize path
    directory_path = File.join('tmp', 'Aruba', 'test-relative-dirs')
    Dir.chdir(directory_path)

    run_system_command('oxen init')

    # Create nested directory structure
    data_path = File.join('data', 'frames')
    FileUtils.mkdir_p(data_path)

    csv_path = File.join('root.csv')
    File.open(csv_path, 'w') do |file|
      file.puts 'id,image,description'
      file.puts '1,/path/to/img1.jpg,test image 1'
      file.puts '2,/path/to/img2.jpg,test image 2'
    end

    run_system_command('oxen add root.csv') 
    run_system_command('oxen commit -m "adding root csv"') 


    # Create a CSV file in the nested directory
    csv_path = File.join(data_path, 'test.csv')
    File.open(csv_path, 'w') do |file|
      file.puts 'id,image,description'
      file.puts '1,/path/to/img1.jpg,test image 1'
      file.puts '2,/path/to/img2.jpg,test image 2'
    end

    Dir.chdir(data_path)


    # Add and commit the CSV file
    run_system_command('oxen add test.csv') 
    run_system_command('oxen commit -m "adding test csv"') 

    # Build command to avoid json-parsing issues
    metadata = {
      "_oxen" => {
        "render" => {
          "func" => "image"
        }
      }
    }

    json_string = metadata.to_json
    root_path = File.join('..', '..', 'root.csv')

    system('oxen', 'schemas', 'add', 'test.csv', '-c', 'image', '-m', json_string) or fail
    system('oxen', 'schemas', 'add', root_path, '-c', 'image', '-m', json_string) or fail
 
    # Verify schema changes 
    status_output = Open3.capture2('oxen status')
    puts status_output
    output_lines = status_output[0].split("\n")
    schema_line = output_lines[10]

    expect(schema_line).to eq("Schemas to be committed") 


    # Return to cli-test 
    parent_path = File.join('..', '..', '..', '..')
    Dir.chdir(parent_path)

  end
end