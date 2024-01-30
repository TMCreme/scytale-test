output "vpc_id" {
  description = "The ID of the VPC"
  value       = module.vpc_and_subnets.vpc_id
}

output "private_subnets" {
  description = "List of IDs of private subnets"
  value       = module.vpc_and_subnets.private_subnets
}

output "public_subnets" {
  description = "List of IDs of public subnets"
  value       = module.vpc_and_subnets.public_subnets
}

output "public_route_table_ids" {
  description = "List of IDs of public route tables"
  value       = module.vpc_and_subnets.public_route_table_ids
}

output "private_route_table_ids" {
  description = "List of IDs of private route tables"
  value       = module.vpc_and_subnets.private_route_table_ids
}

output "nat_ids" {
  description = "List of allocation ID of Elastic IPs created for AWS NAT Gateway"
  value       = module.vpc_and_subnets.nat_ids
}

output "nat_public_ips" {
  description = "List of public Elastic IPs created for AWS NAT Gateway"
  value       = module.vpc_and_subnets.nat_public_ips
}

output "natgw_ids" {
  description = "List of NAT Gateway IDs"
  value       = module.vpc_and_subnets.natgw_ids
}

output "igw_id" {
  description = "The ID of the Internet Gateway"
  value       = module.vpc_and_subnets.igw_id
}


output "ec2_key_pair" {
  description = "Key pair for the ec2 instance"
  value = module.key_pair.key_pair_name
}

output "ec2_scurity_group" {
  description = "Security group 1"
  value = module.main_sg.security_group_id
}

output "ec2_dns_endpoint" {
  description = "Endpoint for the EC2 instance"
  value = module.ec2_instance.public_dns
}

# ami-0694d931cee176e7d
