require 'spec_helper'
require 'pathname'
require 'securerandom'

RSpec.describe 'add - test relative paths', type: :aruba do
  # Helper method to generate unique IDs
  def generate_unique_id
    SecureRandom.hex(4)  # Generates an 8-character hex string
  end

  before(:each) do
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    FileUtils.rm_rf('test-relative-paths')
    FileUtils.rm_rf('test-clone')

  end

  it 'tests oxen add with relative paths from subdirectories' do
    
    # Setup base repo
    directory_path = File.join('tmp', 'aruba', 'test-relative-paths')
    FileUtils.mkdir_p(directory_path)

    # Capitalize path 
    directory_path = File.join('Tmp', 'Aruba', 'test-relative-paths')
    Dir.chdir(directory_path)
    
    run_system_command('oxen init') 

    # Create nested directory structure
    file_path = File.join('hi.txt')
   
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file.'
    end

    images_path = File.join('images', 'test')
    FileUtils.mkdir_p(images_path)
  
    # Add file using relative path from nested directory
    Dir.chdir(images_path)

    parent_path = File.join('..', '..')
    file_path = File.join(parent_path, 'hi.txt')
    run_system_command("oxen add #{file_path}")

    # Create another file in nested directory
    nested_path = File.join('nested.txt')
    File.open(nested_path, 'w') do |file|
      file.puts 'nested'
    end

    # Add file from current directory
    run_system_command("oxen add nested.txt") 

    # Go back to root
    Dir.chdir(parent_path)

    # Verify file contents
    run_system_command('oxen status') 
  
    # Verify file contents
    nested_path = File.join('images', 'test', 'nested.txt')

    expect(File.read(File.join('hi.txt'))).to eq("This is a simple text file.\n")
    expect(File.read(nested_path)).to eq("nested\n")

    
    # Return to cli-test 
    parent_path = File.join('..', '..', '..')
    Dir.chdir(parent_path)
    
  end

  it 'tests oxen add with remote mode repo' do
    
    # Setup base repo
    directory_path = File.join('tmp', 'aruba', 'test-relative-paths')
    FileUtils.mkdir_p(directory_path)

    # Capitalize path 
    # TODO: Workspace/Remote-mode add don't currently support relative, capitalized, or glob paths
    # directory_path = File.join('Tmp', 'Aruba', 'test-relative-paths')
    Dir.chdir(directory_path)
  
    run_system_command('oxen init') 
    first_path = File.join('first_file.txt')
    File.open(first_path, 'a') do |file|
      file.puts 'first_text'
    end

    # Add file from current directory
    run_system_command("oxen add first_file.txt") 

    # Commit file
    run_system_command('oxen commit -m "add first_file.txt"') 
    unique_id = generate_unique_id
    remote_repo_name = "ox/hi-#{unique_id}"
    local_repo_name = "hi-#{unique_id}"

    run_system_command("oxen create-remote --name #{remote_repo_name} --host localhost:3000 --scheme http")
    run_system_command("oxen config --set-remote origin http://localhost:3000/#{remote_repo_name}")


    # Push to remote
    run_system_command("oxen push origin main")

    # Setup repo for clone
    Dir.chdir("..")
    clone_path = "test-clone"
    FileUtils.mkdir_p(clone_path)
    Dir.chdir("test-clone")

     # Clone repo in remote mode
    run_system_command("oxen clone --remote http://localhost:3000/#{remote_repo_name}")
    Dir.chdir(local_repo_name)

    # Create nested directory structure
    file_path = File.join('hi.txt')
   
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file.'
    end

    run_system_command("oxen add #{file_path}")

    # TODO: This doesn't appear to check the output of status beyond it not failing
    # Verify file contents
    run_system_command('oxen status') 
    expect(File.read(File.join('hi.txt'))).to eq("This is a simple text file.\n")

    # Return to cli-test 
    parent_path = File.join('..', '..', '..')
    Dir.chdir(parent_path)
  end
end
