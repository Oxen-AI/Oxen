require 'spec_helper'

RSpec.describe 'rm - test relative paths', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 120
  end

  after(:each) do
    FileUtils.rm_rf('test-relative-paths')

  end

  it 'tests oxen rm with relative paths from subdirectories' do

    # Setup base repo
    directory_path = File.join('tmp', 'aruba', 'test-relative-dirs')
    FileUtils.mkdir_p(directory_path)

    # Capitalize path
    directory_path = File.join('tmp', 'Aruba', 'test-relative-dirs')
    Dir.chdir(directory_path)

    run_system_command('oxen init')

    # Create nested directory structure
    images_path = File.join('images', 'test')
    FileUtils.mkdir_p(images_path)
    
    # Create and commit first set of files
    file_path = File.join('root.txt')
    File.open(file_path, 'w') do |file|
      file.puts 'root file'
    end
    run_system_command('oxen add root.txt') 
    run_system_command('oxen commit -m "adding root file"') 
    
    # Create and commit nested file
    
    test_path = File.join('images', 'test')
    nested_path = File.join(test_path, 'nested.txt')
    File.open(nested_path, 'w') do |file|
      file.puts 'nested file'
    end

    Dir.chdir(test_path)
    run_system_command('oxen add nested.txt') 
    run_system_command('oxen commit -m "adding nested file"') 
    
    # Test removing file from nested directory
    parent_path = File.join('..', '..')
    root_path = File.join(parent_path, 'root.txt')
    run_system_command("oxen rm #{root_path}") 
    
    # Test removing local file
    run_system_command('oxen rm nested.txt') 
    
    # Go back to root and verify files are removed
    Dir.chdir(parent_path)
    run_system_command('oxen status') 
    
    # Should show files as removed in staging
    
    # Files should still exist on disk
    expect(File.exist?(File.join(directory_path, 'root.txt'))).to be false
    expect(File.exist?(File.join(directory_path, nested_path))).to be false

    # Return to cli-test 
    parent_path = File.join('..', '..')
    Dir.chdir(parent_path)

  end

  it 'tests oxen rm with removed path from disk' do
 

    # Setup base repo
    directory_path = File.join('tmp', 'aruba', 'test-relative-dirs')
    FileUtils.mkdir_p(directory_path)

    # Capitalize path
    directory_path = File.join('tmp', 'Aruba', 'test-relative-dirs')
    Dir.chdir(directory_path)

    run_system_command('oxen init') 

    # Create and commit root file
    file_path = File.join('root.txt')
    File.open(file_path, 'w') do |file|
      file.puts 'root file'
    end
    run_system_command('oxen add root.txt') 
    run_system_command('oxen commit -m "adding root file"') 

    # Test removing file before running oxen rm
    FileUtils.rm('root.txt') 
    run_system_command('oxen rm root.txt') 

    # Files should not exist on disk
    expect(File.exist?(File.join('root.txt'))).to be false

    # Return to cli-test 
    parent_path = File.join('..', '..')
    Dir.chdir(parent_path)
  end
end