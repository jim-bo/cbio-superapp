#!/usr/bin/env bash
# One-time setup: Workload Identity Federation for GitHub Actions → GCP.
#
# This creates a WIF pool, provider, and service account so GitHub Actions
# can deploy to Cloud Run without storing GCP credentials as secrets.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - GCP_PROJECT env var set (or pass as first argument)
#   - GITHUB_REPO env var set (or pass as second argument), e.g. "jim-bo/cbio-cli"
#
# Usage:
#   GCP_PROJECT=your-project GITHUB_REPO=owner/repo ./scripts/setup-wif.sh
#   # or
#   ./scripts/setup-wif.sh your-project owner/repo

set -euo pipefail

PROJECT="${1:-${GCP_PROJECT:?Set GCP_PROJECT or pass as first argument}}"
REPO="${2:-${GITHUB_REPO:?Set GITHUB_REPO or pass as second argument}}"
REGION="${GCP_REGION:-us-central1}"

SA_NAME="github-deploy"
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
POOL_NAME="github-actions"
PROVIDER_NAME="github"

echo "=== Workload Identity Federation Setup ==="
echo "Project:  $PROJECT"
echo "Repo:     $REPO"
echo "Region:   $REGION"
echo ""

# Enable required APIs
echo "Enabling APIs..."
gcloud services enable \
  iamcredentials.googleapis.com \
  iam.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  storage.googleapis.com \
  --project "$PROJECT"

# Create service account
echo "Creating service account..."
gcloud iam service-accounts create "$SA_NAME" \
  --display-name="GitHub Actions Deploy" \
  --project="$PROJECT" 2>/dev/null || echo "  (already exists)"

# Grant roles to service account
echo "Granting IAM roles..."
for role in \
  roles/run.admin \
  roles/iam.serviceAccountUser \
  roles/artifactregistry.writer \
  roles/storage.objectViewer; do
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$role" \
    --condition=None \
    --quiet >/dev/null
  echo "  $role"
done

# Create WIF pool
echo "Creating Workload Identity Pool..."
gcloud iam workload-identity-pools create "$POOL_NAME" \
  --location="global" \
  --display-name="GitHub Actions" \
  --project="$PROJECT" 2>/dev/null || echo "  (already exists)"

# Create WIF provider
echo "Creating Workload Identity Provider..."
gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_NAME" \
  --location="global" \
  --workload-identity-pool="$POOL_NAME" \
  --display-name="GitHub OIDC" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository == '${REPO}'" \
  --project="$PROJECT" 2>/dev/null || echo "  (already exists)"

# Get the project number (needed for the provider resource name)
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT" --format="value(projectNumber)")

# Bind WIF to service account
echo "Binding WIF to service account..."
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_NAME}/attribute.repository/${REPO}" \
  --project="$PROJECT" \
  --quiet >/dev/null

# Print the secrets
WIF_PROVIDER="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_NAME}/providers/${PROVIDER_NAME}"

echo ""
echo "==========================================="
echo "Setup complete. Add these GitHub repo secrets:"
echo "==========================================="
echo ""
echo "  WIF_PROVIDER:         $WIF_PROVIDER"
echo "  WIF_SERVICE_ACCOUNT:  $SA_EMAIL"
echo "  GCP_PROJECT:          $PROJECT"
echo ""
echo "Go to: https://github.com/${REPO}/settings/secrets/actions"
echo ""
echo "Then push to main to trigger the deploy workflow."
