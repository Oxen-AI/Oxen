# Oxen CLI Tests

Tests use RSpec and Aruba

## Setup

1. From the `cli-test` folder, run `bundle install` to install the required gems.

2. (Currently only needed for the performance test) Create a `.env` file in the `cli-test` folder containing the credentials for the test user (@test):

    ```
    OXEN_API_KEY=<test-user-API-key>
    ```

## Running all tests
```bash
$ bundle exec rspec
```

## Running specific tests
```bash
$ bundle exec rspec spec/spec_remote_hub/remote_hub_remove_image_spec.rb
```
