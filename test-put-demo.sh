#!/bin/bash

# Demo script to test Oxen PUT functionality
# Usage: ./test-put-demo.sh /path/to/oxen/target/debug [workdir_name]

# Note: We'll handle errors manually to prevent premature exit
# set -e  # Exit on any error - disabled for better error handling

# Check if target directory is provided
if [ -z "$1" ]; then
    echo "Usage: $0 /path/to/oxen/target/debug [workdir_name]"
    echo "  workdir_name: Optional name for working directory (default: oxen-put-demo)"
    exit 1
fi

OXEN_TARGET_DIR="$1"
WORKDIR="${2:-oxen-put-demo}"
OXEN_CLIENT="$OXEN_TARGET_DIR/oxen"
OXEN_SERVER="$OXEN_TARGET_DIR/oxen-server"

pkill oxen-server
rm -rf eeee/oxen-put-demo/
cd eeee

# Check if binaries exist
if [ ! -f "$OXEN_CLIENT" ]; then
    echo "Error: oxen CLI not found at $OXEN_CLIENT"
    exit 1
fi

if [ ! -f "$OXEN_SERVER" ]; then
    echo "Error: oxen-server not found at $OXEN_SERVER"
    exit 1
fi

echo "=== Oxen PUT Demo Script ==="
echo "Using Oxen binaries from: $OXEN_TARGET_DIR"
echo "Working directory: $WORKDIR"

# Create and enter working directory
echo
echo "Setting up working directory: $WORKDIR"
rm -rf "$WORKDIR"
mkdir -p "$WORKDIR"
cd "$WORKDIR"

# Step 1: Create user and get auth token (will be done after server initialization)
echo
echo "Step 1: Server initialization and user creation will happen in Step 2..."

# Set up user name for this demo
USER_NAME="$WORKDIR"
echo "Using user name: $USER_NAME"

# Step 2: Start the server in background
echo
echo "Step 2: Starting Oxen server in background..."
SERVER_PID=""
cleanup() {
    if [ ! -z "$SERVER_PID" ]; then
        echo "Stopping server (PID: $SERVER_PID)..."
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
    # Clean up test files
    cd ..
    rm -rf "$WORKDIR"
}
# trap cleanup EXIT

# Set sync directory for demo
export SYNC_DIR="./demo-data"
mkdir -p "$SYNC_DIR"

# Debug: Show sync directory setup
echo "SYNC_DIR set to: $SYNC_DIR"
echo "Absolute SYNC_DIR: $(realpath $SYNC_DIR)"

# IMPORTANT: Set up authentication BEFORE starting the server
echo "Setting up authentication infrastructure before starting server..."

# Set up the server's access key infrastructure in the sync directory
# The server expects keys to be in sync_dir/.oxen/
mkdir -p "$SYNC_DIR/.oxen/keys"

# Create the secret key file that AccessKeyManager expects
SECRET="test-secret-key-for-oxen-testing-only"
echo "$SECRET" > "$SYNC_DIR/.oxen/SECRET_KEY_BASE"
echo "Created SECRET_KEY_BASE file at: $SYNC_DIR/.oxen/SECRET_KEY_BASE"

# Debug: Verify the file was created correctly
if [ -f "$SYNC_DIR/.oxen/SECRET_KEY_BASE" ]; then
    echo "✅ SECRET_KEY_BASE file exists"
    echo "File contents: $(cat "$SYNC_DIR/.oxen/SECRET_KEY_BASE")"
    echo "File permissions: $(ls -la "$SYNC_DIR/.oxen/SECRET_KEY_BASE")"
else
    echo "❌ SECRET_KEY_BASE file not found!"
fi

# Initialize the RocksDB database for access keys
echo "Initializing AccessKeyManager database..."
echo "Creating keys database directory: $SYNC_DIR/.oxen/keys"
mkdir -p "$SYNC_DIR/.oxen/keys"

# We need to start the server without --auth first to let it initialize the database
echo "Starting server WITHOUT --auth first to initialize database..."
$OXEN_SERVER start &
TEMP_SERVER_PID=$!
echo "Temporary server started with PID: $TEMP_SERVER_PID"
echo "Waiting for database initialization..."
sleep 5

# Force database creation by running add-user command
echo "Creating main user for testing..."
USER_OUTPUT=$($OXEN_SERVER add-user --email demo@example.com --name "Test User" 2>&1)
echo "User creation result: $USER_OUTPUT"

# Extract the auth token from the output
if echo "$USER_OUTPUT" | grep -q "eyJ"; then
    TOKEN=$(echo "$USER_OUTPUT" | grep "eyJ" | head -1 | tr -d ' ')
    echo "✅ Extracted token from command output"
    echo "Generated auth token: ${TOKEN:0:50}..."
else
    echo "❌ Could not extract token from output: $USER_OUTPUT"
    exit 1
fi

# Check if database was created
if [ -f "$SYNC_DIR/.oxen/keys/CURRENT" ]; then
    echo "✅ RocksDB database initialized successfully"
else
    echo "❌ RocksDB database not created, listing keys directory:"
    ls -la "$SYNC_DIR/.oxen/keys/" 2>/dev/null || echo "Keys directory is empty or missing"
fi

# Stop the temporary server
echo "Stopping temporary server to restart with --auth..."
kill $TEMP_SERVER_PID 2>/dev/null || true
wait $TEMP_SERVER_PID 2>/dev/null || true
sleep 2

# Check if port 3000 is already in use
if netstat -ln 2>/dev/null | grep -q ":3000 "; then
    echo "Warning: Port 3000 is already in use. Attempting to use existing server..."
    echo "If this fails, please stop the existing server first."
    sleep 1
else
    # Start server in background AFTER setting up auth
    echo "Starting server with SYNC_DIR=$SYNC_DIR and --auth flag..."
    $OXEN_SERVER start --auth &
    SERVER_PID=$!
    echo "Server started with PID: $SERVER_PID"
    echo "Waiting for server to be ready..."
    sleep 5
fi

# Step 3: Create local repository
echo
echo "Step 3: Creating local repository..."
rm -rf demo-repo
mkdir demo-repo
cd demo-repo

# Initialize repo
$OXEN_CLIENT init

# Configure user
$OXEN_CLIENT config --name "$USER_NAME" --email demo@example.com

# Create test file
echo "Hello, this is the original content!" > test.txt
echo "Original content: $(cat test.txt)"

# Add and commit
$OXEN_CLIENT add test.txt
$OXEN_CLIENT commit -m "Initial commit with test.txt"

# Step 4: Configure remote and auth
echo
echo "Step 4: Configuring remote repository..."
REPO_NAME="test-repo"
$OXEN_CLIENT config --set-remote origin http://localhost:3000/$USER_NAME/$REPO_NAME
echo "Setting auth token for localhost:3000..."
$OXEN_CLIENT config --auth localhost:3000 "$TOKEN"

# Debug: Check if auth was set correctly by looking at config files
echo "Checking auth configuration..."
if [ -f ~/.config/oxen/auth_config.toml ]; then
    echo "Auth config file exists:"
    cat ~/.config/oxen/auth_config.toml
else
    echo "No auth config file found at ~/.config/oxen/auth_config.toml"
fi

# Step 5: Create remote repository
echo
echo "Step 5: Creating remote repository..."
echo "Attempting to create remote repository $USER_NAME/$REPO_NAME..."

# Run create-remote with timeout and better error handling
echo "Running: $OXEN_CLIENT create-remote --name $USER_NAME/$REPO_NAME --host localhost:3000 --scheme http"
echo "This may take a moment..."
timeout 30s $OXEN_CLIENT create-remote --name $USER_NAME/$REPO_NAME --host localhost:3000 --scheme http > create_output.log 2>&1
CREATE_EXIT_CODE=$?

echo "Create-remote exit code: $CREATE_EXIT_CODE"
if [ -f create_output.log ]; then
    echo "Create remote output:"
    cat create_output.log
    CREATE_OUTPUT=$(cat create_output.log)
else
    echo "No output log file created"
    CREATE_OUTPUT=""
fi

# Analyze the result
if [ $CREATE_EXIT_CODE -eq 124 ]; then
    echo "❌ Create-remote command timed out after 30 seconds"
elif [ $CREATE_EXIT_CODE -ne 0 ]; then
    echo "❌ Create-remote command failed with exit code: $CREATE_EXIT_CODE"
else
    echo "✅ Create-remote command completed successfully"
fi

# Exit if create-remote failed
if [ $CREATE_EXIT_CODE -ne 0 ] || echo "$CREATE_OUTPUT" | grep -qi "unauthenticated\|error"; then
    echo "❌ create-remote failed - this is required for the demo to work"
    echo "Exit code: $CREATE_EXIT_CODE"
    echo "Output: $CREATE_OUTPUT"
    exit 1
fi

echo "✅ Repository created successfully via CLI"

# Clean up log file
rm -f create_output.log

# Step 6: Push repository
echo
echo "Step 6: Pushing repository to server..."
PUSH_OUTPUT=$($OXEN_CLIENT push origin main 2>&1)
echo "Push output: $PUSH_OUTPUT"

# Wait a moment for server to process
echo "Waiting for server to process repository..."
sleep 2

# Step 7: Test initial GET request
echo
echo "Step 7: Testing initial GET request..."

# Debug repository variables
echo "Debug: USER_NAME='$USER_NAME', REPO_NAME='$REPO_NAME'"

# First, let's debug by listing files in the repository
echo "Debugging - listing files in repository:"
echo "URL: http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/dir/main"
LIST_RESPONSE=$(curl -s -H "Authorization: Bearer $TOKEN" \
    "http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/dir/main")
echo "Repository files: $LIST_RESPONSE"

# TODO should contain `"entries":[{"filename":"test.txt"`

echo "GET request to retrieve original file content:"
echo "URL for curl GET: http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/file/main/test.txt"

# Test GET without token first
echo "Testing GET without token:"
GET_NO_TOKEN=$(curl -s -i "http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/file/main/test.txt")
echo "$GET_NO_TOKEN" | head -10

echo
echo "Testing GET with token:"
GET_RESPONSE=$(curl -s -i -H "Authorization: Bearer $TOKEN" \
    "http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/file/main/test.txt")
echo "GET response with headers:"
echo "$GET_RESPONSE"

# Extract oxen-revision-id from headers (this should be the commit hash for oxen-based-on)
OXEN_REVISION_ID=$(echo "$GET_RESPONSE" | grep -i "oxen-revision-id:" | cut -d':' -f2 | tr -d ' \r\n')
echo "Extracted oxen-revision-id (commit hash): '$OXEN_REVISION_ID'"

# Let's also check what the ETag contains (might be the file hash we need)
ETAG=$(echo "$GET_RESPONSE" | grep -i "etag:" | cut -d':' -f2 | tr -d ' \r\n"' | sed 's/"//g')
echo "Extracted ETag: '$ETAG'"

# The ETag might contain multiple parts separated by colons - try extracting the first part
if [[ "$ETAG" == *":"* ]]; then
    FILE_HASH=$(echo "$ETAG" | cut -d':' -f1)
    echo "Extracted file hash from ETag: '$FILE_HASH'"
else
    FILE_HASH="$ETAG"
fi

# Debug: Show all headers to understand what we have
echo "All response headers:"
echo "$GET_RESPONSE" | head -20

# Step 8: Test PUT request
echo
echo "Step 8: Testing PUT request to overwrite file..."
NEW_CONTENT="This is the NEW content after PUT request!"

# Create temporary file for multipart upload
TEMP_FILE=$(mktemp)
echo "$NEW_CONTENT" > "$TEMP_FILE"

# Debug the PUT request
echo "Debug PUT request:"
echo "  URL: http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/file/main/test.txt"
echo "  Token: ${TOKEN:0:50}..."
echo "  Revision ID: $OXEN_REVISION_ID"

# Use multipart form data with the commit hash from oxen-revision-id
# PUT to the full file path, replicating the rust test logic
echo "Testing PUT with multipart form data to full file path..."
PUT_RESPONSE=$(curl -s -X PUT \
    -H "Authorization: Bearer $TOKEN" \
    -H "oxen-based-on: $OXEN_REVISION_ID" \
    -F "file=@$TEMP_FILE;filename=test.txt;type=text/plain" \
    -F "message=Update test.txt via PUT request" \
    "http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/file/main/test.txt")

# Clean up temporary file
rm -f "$TEMP_FILE"
echo "PUT Response: $PUT_RESPONSE"

# Check if PUT was successful
if echo "$PUT_RESPONSE" | grep -qi "error\|failed"; then
    echo "❌ PUT request failed"
else
    echo "✅ PUT worked"
fi

# Step 9: Verify PUT worked with another GET
echo
echo "Step 9: Verifying PUT worked with another GET request..."

# First, let's check the repository state
echo "Checking repository files after PUT:"
LIST_AFTER_PUT=$(curl -s -H "Authorization: Bearer $TOKEN" \
    "http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/dir/main")
echo "Repository files after PUT: $LIST_AFTER_PUT" | head -5

echo
echo "GET request after PUT to confirm content changed:"
sleep 3 # Allow server time to process the commit
GET_RESPONSE_AFTER=$(curl -s -H "Authorization: Bearer $TOKEN" \
    "http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/file/main/test.txt")
echo "Updated content: $GET_RESPONSE_AFTER"

# Compare content to verify PUT worked
if [ "$GET_RESPONSE_AFTER" = "$NEW_CONTENT" ]; then
    echo "✅ Content successfully updated via PUT!"
elif echo "$GET_RESPONSE_AFTER" | grep -q "$NEW_CONTENT"; then
    echo "✅ Content contains expected update!"
else
    echo "❌ Content was not updated properly"
    echo "Expected: $NEW_CONTENT"
    echo "Got: $GET_RESPONSE_AFTER"
fi

# Step 10: Test another PUT to ensure we can update a file multiple times
echo
echo "Step 10: Testing a second PUT to the same file..."
FINAL_CONTENT="Final test content to verify PUT endpoint is working correctly!"

# IMPORTANT: We must get the LATEST revision ID before this second PUT
echo "Getting latest revision ID for second PUT..."
GET_RESPONSE_FOR_SECOND_PUT=$(curl -s -i -H "Authorization: Bearer $TOKEN" \
    "http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/file/main/test.txt")
LATEST_OXEN_REVISION_ID=$(echo "$GET_RESPONSE_FOR_SECOND_PUT" | grep -i "oxen-revision-id:" | cut -d':' -f2 | tr -d ' \r\n')
echo "Extracted latest oxen-revision-id: '$LATEST_OXEN_REVISION_ID'"

if [ -z "$LATEST_OXEN_REVISION_ID" ]; then
    echo "❌ Could not get latest revision ID. Skipping second PUT."
else
    # Create temporary file for final multipart upload
    FINAL_TEMP_FILE=$(mktemp)
    echo "$FINAL_CONTENT" > "$FINAL_TEMP_FILE"

    FINAL_PUT_RESPONSE=$(curl -s -X PUT \
        -H "Authorization: Bearer $TOKEN" \
        -H "oxen-based-on: $LATEST_OXEN_REVISION_ID" \
        -F "file=@$FINAL_TEMP_FILE;filename=test.txt;type=text/plain" \
        -F "message=Final test update via PUT request" \
        "http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/file/main/test.txt")

    # Clean up temporary file
    rm -f "$FINAL_TEMP_FILE"
    echo "Final PUT Response: $FINAL_PUT_RESPONSE"

    # Final verification
    sleep 3 # Allow server time to process the commit
    FINAL_GET_RESPONSE=$(curl -s -H "Authorization: Bearer $TOKEN" \
        "http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/file/main/test.txt")
    echo "Final content: $FINAL_GET_RESPONSE"

    if [ "$FINAL_GET_RESPONSE" = "$FINAL_CONTENT" ]; then
        echo "✅ Final PUT verification successful!"
    else
        echo "❌ Final PUT verification failed"
    fi
fi

echo
echo "=== Demo completed! ==="
echo "Summary:"
echo "- ✅ Server setup and authentication working"

# Check if repository was actually accessible
if echo "$GET_RESPONSE" | grep -q "resource_not_found"; then
    echo "- ❌ Repository not accessible via API"
    REPO_ACCESSIBLE=false
else
    echo "- ✅ Repository created and accessible"
    REPO_ACCESSIBLE=true
fi

# Check if GET requests worked
if echo "$GET_RESPONSE" | grep -q "Hello, this is the original content"; then
    echo "- ✅ GET requests working"
    GET_WORKING=true
else
    echo "- ❌ GET requests failed"
    GET_WORKING=false
fi

# Check if PUT requests worked
if echo "$PUT_RESPONSE" | grep -qi "error\|failed\|resource_not_found"; then
    echo "- ❌ PUT requests failed"
    PUT_WORKING=false
else
    echo "- ✅ PUT requests working"
    PUT_WORKING=true
fi

# Check if content was actually updated
if [ "$GET_RESPONSE_AFTER" = "$NEW_CONTENT" ]; then
    echo "- ✅ Content updates verified"
    CONTENT_UPDATED=true
else
    echo "- ❌ Content updates failed"
    CONTENT_UPDATED=false
fi
echo
echo "Technical details:"
echo "- Auth token: $TOKEN"
echo "- Server running on: http://localhost:3000"
echo "- Repository: $USER_NAME/$REPO_NAME"
echo "- PUT endpoint: http://localhost:3000/api/repos/$USER_NAME/$REPO_NAME/file/main/test.txt"
echo

# Final assessment
if [ "$PUT_WORKING" = true ] && [ "$CONTENT_UPDATED" = true ]; then
    echo "🎉 Your PUT feature implementation is working correctly!"
elif [ "$REPO_ACCESSIBLE" = false ]; then
    echo "⚠️  Repository setup failed - PUT feature could not be tested"
    echo "   This is likely a server configuration issue, not a PUT implementation problem"
else
    echo "❌ PUT feature needs debugging"
fi

echo
echo "Next steps:"
if [ "$REPO_ACCESSIBLE" = false ]; then
    echo "1. Debug why repository is not accessible via API"
    echo "2. Check server logs for repository creation issues"
    echo "3. Try manual repository setup in server data directory"
else
    echo "1. Repository is accessible - you can test PUT manually with the token above"
    echo "2. Use curl commands to test specific scenarios"
fi

# Return to parent directory
cd ..
cd ..
