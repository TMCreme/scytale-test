variable "vpc_name" {
  type = string
  description = "Name of the VPC"
  default = "higherheights"
}

variable "state_bucket" {
  type = string
  description = "The name of the s3 bucket to keep track of infra state"
  default = "staging-state-bucket"
}

variable "main_region" {
  type = string
  description = "The main region for the infra"
  default = "eu-west-1"
}

variable "ec2_key_name" {
  type = string
  description = "Name of the EC2 key pair"
  default = "scytale-keypair"
}

variable "ec2_instance_name" {
  type = string
  description = "The name of the EC2 instance"
  default = "backend-instance"
}

variable "ec2_instance_type" {
  type = string
  default = "t3.2xlarge"
}

variable "ec2_ami_id" {
  type = string
  default = "ami-0694d931cee176e7d"
}

variable "account_id" {
  type = string
  description = "Account ID for the AWS account"
}

variable "enable_nat_gateway" {
  description = "Should be true if you want to provision NAT Gateways"
  type = bool
  default = true
}

variable "single_nat_gateway" {
  description = "Should be true if you want a single shared NAT gateway accross the private networks"
  type = bool
  default = false
}

variable "one_nat_gateway_per_az" {
  description = "Should be true if you only one NAT Gateway per AZ"
  type = bool
  default = true
}

variable "enable_dns_hostnames" {
  description = "Should be try to enable DNS hostnames in the VPC"
  type = bool
  default = true
}

variable "enable_dns_support" {
  description = "Should be true to enable DNS support inthe VPC"
  type = bool
  default = true
}

variable "tags" {
  description = "A mapping of the tags to assign to all resources"
  type = map(string)
  default = {}
}

variable "create_igw" {
  description = "Controls if an Internet Gateway is created for pubic subnets and related routes that connect them"
  type = bool
  default = true
}

variable "instance_tenancy" {
  description = "A tenancy option for instances launched into the VPC"
  type = string
  default = "default"
}

variable "iam_policy_name" {
  type = string
  description = "Name of the Iam policy for full SSM access"
  default = "customfullaccesspolicy"
}


