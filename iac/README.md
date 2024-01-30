# Configuring the EC2 instance with Terraform

## Description 
The terraform script is for provisioning the infrastructure that runs the application. 

## Running the Infrastructure As Code
* Make sure you have configured the AWS CLI credentials for your computer
* Initialize the terraform environment with 
```sh
terraform init
```
* In case you have multiple AWS Profiles on your system, you can specify the particular profile by using the following. Make sure to replace the variable `{profile_name}` with your profile name
```sh 
AWS_PROFILE={profile_name} terraform init
```
* Run the plan command for terraform to validate and aggregate the resources to create
```sh 
terraform plan
```
* Apply the configuration if everything is working properly. This command creates the resources defined in the configuration such as a VPC, Subnets, Security groups, IAM roles and instance profile, etc
```sh
terraform apply -var account_id="{AWS_account_ID}"
```

