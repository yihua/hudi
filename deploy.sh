#!/bin/bash

# --- Configuration ---
# Define the full Maven command to execute.
# We enclose it in quotes to treat it as a single unit when running.
MVN_COMMAND="mvn deploy -DdeployArtifacts=true -DskipTests -DretryFailedDeploymentCount=10 -Dmaven.wagon.http.connectionTimeout=600000 -Dmaven.wagon.http.readTimeout=900000 -Dmaven.wagon.http.retryHandler.count=20 -Dscala-2.12 -Dspark3.5 -pl packaging/hudi-integ-test-bundle"

# Define the sleep duration (in seconds) between retries
RETRY_DELAY=10

# Initialize attempt counter
ATTEMPTS=0

echo "Starting continuous deployment attempt for module: packaging/hudi-integ-test-bundle"
echo "Command: $MVN_COMMAND"

# The 'until' loop executes the command and repeats the loop body as long as the command fails (exit code != 0).
# Once the command succeeds (exit code 0), the loop terminates.
until $MVN_COMMAND; do
    EXIT_CODE=$?
    ATTEMPTS=$((ATTEMPTS + 1))
    
    echo "--------------------------------------------------------"
    echo "$(date): Deployment failed with exit code $EXIT_CODE on attempt $ATTEMPTS."
    echo "Retrying in $RETRY_DELAY seconds..."
    echo "--------------------------------------------------------"
    
    sleep $RETRY_DELAY
done

# Success message after the loop breaks
echo "========================================================"
echo "SUCCESS! Deployment completed successfully."
echo "Total attempts required: $((ATTEMPTS + 1)) (The final successful run + $ATTEMPTS previous failures)."
echo "========================================================"

# Exit with success status
exit 0

