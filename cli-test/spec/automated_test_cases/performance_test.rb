require 'spec_helper'
require 'dotenv'

RSpec.describe 'test', type: :aruba do
  before(:each) do
    aruba.config.exit_timeout = 6000 # Set the timeout to 600 0seconds
  end

  after(:each) do
    FileUtils.rm_rf('test-small-repo')
  
  end

  it 'tests oxen init, add, commit, and push with a small file' do
    # Setup
    repo_path = File.join('tmp', 'aruba', 'test-small-repo')
    FileUtils.mkdir_p(repo_path)
    Dir.chdir(repo_path)

    # Generate image repository
    script_path = File.join('..', 'benchmark', 'generate_image_repo.py')
    run_system_command("python #{script_path} --output_dir ~/test-small-repo/Data/10k_images --num_images 10000 --num_dirs 10 --image_size 128 128")

    # Initialize the repository
    init_time = measure_time('oxen init')
    puts "oxen init command took: #{init_time} seconds"
    expect(init_time).to be < 7.0


    # Add the file
    add_time = measure_time('oxen add .')
    puts "oxen add command took: #{add_time} seconds"
    expect(add_time).to be < 77.0

    # Commit the file
    commit_time = measure_time('oxen commit -m "Add small file"')
    puts "oxen commit command took: #{commit_time} seconds"
    expect(commit_time).to be < 85.0
    # Create the remote repository
    create_remote_time = measure_time('oxen create-remote --name EloyMartinez/performance-test --host dev.hub.oxen.ai --scheme https --is_public')
    puts "oxen create-remote command took: #{create_remote_time} seconds"
    expect(create_remote_time).to be < 7.5

    # Set the remote
    set_remote_time = measure_time('oxen config --set-remote origin https://dev.hub.oxen.ai/EloyMartinez/performance-test')
    puts "oxen config --set-remote command took: #{set_remote_time} seconds"
    expect(set_remote_time).to be < 5.0

    # Push the file
    push_time = measure_time('oxen push')
    puts "oxen push command took: #{push_time} seconds"
    expect(push_time).to be < 1000.0

    Dir.chdir('..')

    # Clone the repository
    clone_time = measure_time('oxen clone https://dev.hub.oxen.ai/EloyMartinez/performance-test')
    puts "oxen clone command took: #{clone_time} seconds"
    expect(clone_time).to be < 700.0

    directory_path = 'tmp/aruba/performance-test'
    file_path = File.join(directory_path, 'simple.txt')

    # Ensure the directory exists
    FileUtils.mkdir_p(directory_path)

    # Append to 'simple.txt'
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file. Edited remotely.'
    end

    Dir.chdir('performance-test')
    # Add, commit, and push the changes
    add_simple_time = measure_time('oxen add simple.txt')
    puts "oxen add simple.txt command took: #{add_simple_time} seconds"
    expect(add_simple_time).to be < 5.0

    commit_simple_time = measure_time('oxen commit -m "Edit simple text file remotely"')
    puts "oxen commit command took: #{commit_simple_time} seconds"
    expect(commit_simple_time).to be < 5.0

    push_simple_time = measure_time('oxen push')
    puts "oxen push command took: #{push_simple_time} seconds"
    expect(push_simple_time).to be < 300.0

    # Pull the changes from test-repo
    Dir.chdir('..')
    Dir.chdir('test-small-repo')
    pull_time = measure_time('oxen pull')
    puts "oxen pull command took: #{pull_time} seconds"
    expect(pull_time).to be < 125.0

    # Checkout to a new branch
    checkout_time = measure_time('oxen checkout -b second_branch')
    puts "oxen checkout command took: #{checkout_time} seconds"
    expect(checkout_time).to be < 7.0

    directory_path = File.join('tmp', 'aruba', 'test-small-repo')
    file_path = File.join(directory_path, 'simple.txt')

    # Ensure the directory exists
    FileUtils.mkdir_p(directory_path)

    # Append to 'simple.txt'
    File.open(file_path, 'a') do |file|
      file.puts 'This is a simple text file. Edited remotely. this is a second change'
    end

    # Add, commit, and push the second change
    add_second_change_time = measure_time('oxen add simple.txt')
    puts "oxen add simple.txt command took: #{add_second_change_time} seconds"
    expect(add_second_change_time).to be < 5.0

    commit_second_change_time = measure_time('oxen commit -m "Edit simple text file remotely second time"')
    puts "oxen commit command took: #{commit_second_change_time} seconds"
    expect(commit_second_change_time).to be < 5.0

    push_second_change_time = measure_time('oxen push')
    puts "oxen push command took: #{push_second_change_time} seconds"
    expect(push_second_change_time).to be < 20.0

    # Checkout main branch and then back to second branch
    checkout_main_branch_time = measure_time('oxen checkout main')
    puts "oxen checkout main command took: #{checkout_main_branch_time} seconds"
    expect(checkout_main_branch_time).to be < 100.0

    checkout_second_branch_time = measure_time('oxen checkout second_branch')
    puts "oxen checkout second_branch command took: #{checkout_second_branch_time} seconds"
    expect(checkout_second_branch_time).to be < 100.0

    # Restore the file from the main branch
    restore_second_change_time = measure_time('oxen restore --source main simple.txt')
    puts "oxen restore command took: #{restore_second_change_time} seconds"
    expect(restore_second_change_time).to be < 7.0

    checkout_main_branch_time = measure_time('oxen checkout main')
    puts "oxen checkout main command took: #{checkout_main_branch_time} seconds"
    expect(checkout_main_branch_time).to be < 100.0

    FileUtils.mkdir_p('files')

      directory_path = File.join('tmp', 'aruba', 'test-small-repo') 

      30.times do |i|
        file_path = File.join('README.md')
        File.open(file_path, 'w') do |file|
          file.puts "\n\nnew Commit #{i}"
        end
        file_path = File.join("#{i}.txt")
        File.open(file_path, 'w') do |file|
          file.puts "hello #{i} world"
        end
        run_system_command('oxen add .')
        run_system_command("oxen commit -m \"commit #{i}\"")
        puts "Completed commit #{i + 1} of 30"
      end


    # Measure time for the push command
    push_time = measure_time('oxen push')
    puts "oxen push command took: #{push_time} seconds"
    expect(push_time).to be < 60.0

    # Return to cli-test 
    parent_path = File.join('..', '..')
    Dir.chdir(parent_path)

  end

  def measure_time(command)
    start_time = Time.now
    run_system_command(command)
    end_time = Time.now
    end_time - start_time
  end
end
