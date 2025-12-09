# Day 1 Setup: Getting Started with Databricks

## Prerequisites

- Email address for account creation
- Web browser (Chrome, Firefox, Safari, or Edge)
- Internet connection

## Setup Steps

### Step 1: Create Databricks Account

**For Community Edition (Recommended for this bootcamp)**:

1. Navigate to [https://community.cloud.databricks.com/](https://community.cloud.databricks.com/)
2. Click "Sign Up"
3. Enter your details:
   - First Name
   - Last Name
   - Email
   - Password
4. Accept terms and conditions
5. Click "Sign Up"
6. Check your email for verification link
7. Click the verification link
8. Log in to your workspace

**For Trial Account (Optional)**:

1. Navigate to [https://databricks.com/try-databricks](https://databricks.com/try-databricks)
2. Choose your cloud provider (AWS, Azure, or GCP)
3. Fill in company and contact information
4. Follow cloud-specific setup instructions
5. Complete trial activation

### Step 2: Explore the Workspace

1. **Log in** to your Databricks workspace
2. **Familiarize yourself** with the left sidebar:
   - Click each icon to see what it does
   - Hover over icons to see tooltips
3. **Navigate to Workspace**:
   - Click "Workspace" in the left sidebar
   - Find your user folder (your email address)
   - This is your personal workspace

### Step 3: Create Your First Folder

1. In the Workspace view, navigate to your user folder
2. Click the dropdown arrow next to your folder name
3. Select "Create" → "Folder"
4. Name it: `30-days-bootcamp`
5. Click "Create"

### Step 4: Create Your First Notebook

1. Navigate to your `30-days-bootcamp` folder
2. Click the dropdown arrow
3. Select "Create" → "Notebook"
4. Configure:
   - **Name**: `Day 1 - Workspace Exploration`
   - **Default Language**: SQL
   - **Cluster**: Leave as "None" for now (we'll create one tomorrow)
5. Click "Create"

### Step 5: Explore Notebook Features

1. **Add a markdown cell**:
   - In the first cell, type: `%md`
   - Press Enter
   - Type: `# My First Databricks Notebook`
   - This creates a markdown header

2. **Add a SQL cell**:
   - Create a new cell (click "+ Code" or press B)
   - Type: `SELECT "Hello Databricks!" as greeting`
   - Note: You can't run this yet without a cluster

3. **Explore the notebook menu**:
   - File menu: Save, export, version history
   - View menu: Display options
   - Run menu: Execution options

### Step 6: Explore Sample Notebooks

1. Click "Workspace" in the left sidebar
2. Navigate to "Workspace" → "Shared"
3. Look for "Sample Notebooks" or "Getting Started" folders
4. Open a sample notebook to see examples
5. Explore the code and markdown cells

### Step 7: Customize Your Workspace

1. Click your profile icon (top right)
2. Select "User Settings"
3. Explore settings:
   - **Developer**: API tokens, Git integration
   - **Compute**: Default cluster settings
   - **Notebook**: Default language, autosave
4. Set your preferences

## Verification Checklist

- [ ] Successfully logged in to Databricks workspace
- [ ] Explored the left sidebar and main navigation
- [ ] Created a folder in your user workspace
- [ ] Created your first notebook
- [ ] Added markdown and SQL cells to notebook
- [ ] Explored sample notebooks
- [ ] Reviewed user settings

## Troubleshooting

### Can't access workspace
- Check your email for verification link
- Clear browser cache and cookies
- Try a different browser
- Check internet connection

### Can't create notebook
- Ensure you're in your user folder or a folder you have permissions for
- Try refreshing the page
- Log out and log back in

### UI looks different
- Databricks updates their UI regularly
- Core functionality remains the same
- Check Databricks documentation for latest UI guide

## What's Next?

You're now ready to start Day 1 exercises! The notebook you created will be used for today's hands-on practice.

Tomorrow, we'll create a cluster and start running code.

## Additional Setup (Optional)

### Install Databricks CLI (Optional)
```bash
pip install databricks-cli
databricks configure --token
```

### Set up Git Integration (Optional)
1. Go to User Settings → Developer → Git Integration
2. Connect your GitHub/GitLab/Bitbucket account
3. Generate personal access token
4. Save configuration

These are optional and not required for the bootcamp, but useful for advanced workflows.
