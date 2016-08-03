package aws

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/emr"
	"github.com/hashicorp/terraform/helper/acctest"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
	"log"
	"testing"
	"time"
)

func TestAccAWSEmrCluster_basic(t *testing.T) {
	var jobFlow emr.RunJobFlowOutput
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckAWSEmrDestroy,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testAccAWSEmrClusterConfig,
				Check:  testAccCheckAWSEmrClusterExists("aws_emr.tf-test-cluster", &jobFlow),
			},
		},
	})
}

func testAccCheckAWSEmrDestroy(s *terraform.State) error {
	conn := testAccProvider.Meta().(*AWSClient).emrconn

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "aws_emr" {
			continue
		}

		params := &emr.DescribeClusterInput{
			ClusterId: aws.String(rs.Primary.ID),
		}

		describe, err := conn.DescribeCluster(params)

		if err == nil {
			if describe.Cluster != nil &&
				*describe.Cluster.Status.State == "WAITING" {
				return fmt.Errorf("EMR Cluster still exists")
			}
		}

		providerErr, ok := err.(awserr.Error)
		if !ok {
			return err
		}

		log.Printf("[ERROR] %v", providerErr)
	}

	return nil
}

func testAccCheckAWSEmrClusterExists(n string, v *emr.RunJobFlowOutput) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("No cluster id set")
		}
		conn := testAccProvider.Meta().(*AWSClient).emrconn
		_, err := conn.DescribeCluster(&emr.DescribeClusterInput{
			ClusterId: aws.String(rs.Primary.ID),
		})
		if err != nil {
			return fmt.Errorf("EMR error: %v", err)
		}
		return nil
	}
}

func testAccCheckAWSEmrCluster_ready(n string, v *emr.RunJobFlowOutput) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Not found: %s", n)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("No cluster id set")
		}

		params := &emr.DescribeClusterInput{
			ClusterId: aws.String(rs.Primary.ID),
		}
		conn := testAccProvider.Meta().(*AWSClient).emrconn

		return resource.Retry(40*time.Minute, func() *resource.RetryError {
			describe, err := conn.DescribeCluster(params)

			if err == nil {
				if describe.Cluster != nil &&
					*describe.Cluster.Status.State == "WAITING" {
					return nil
				}

				return resource.RetryableError(fmt.Errorf(
					"Waiting for cluster to be running: %v", n))
			}

			return resource.NonRetryableError(
				fmt.Errorf("Error retrieving Emr status: %s", err))
		})
	}
}

func TestAccAWSEmrCore_resize(t *testing.T) {
	var jobFlow emr.RunJobFlowOutput

	resource.Test(t, resource.TestCase{
		PreCheck:      func() { testAccPreCheck(t) },
		IDRefreshName: "aws_emr.foo",
		Providers:     testAccProviders,
		CheckDestroy:  testAccCheckAWSEmrDestroy,
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testAccAWSEmrConfigCore,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSEmrClusterExists("aws_emr.foo", &jobFlow),
					testAccCheckAWSEmrCluster_ready("aws_emr.foo", &jobFlow),
					resource.TestCheckResourceAttr(
						"aws_emr.foo", "core_instance_count", "1"),
				),
			},

			resource.TestStep{
				Config: testAccAWSEmrConfigCore_resize,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckAWSEmrClusterExists("aws_emr.foo", &jobFlow),
					resource.TestCheckResourceAttr(
						"aws_emr.foo", "core_instance_count", "2"),
				),
			},
		},
	})
}

var testAccAWSEmrClusterConfig = fmt.Sprintf(`
provider "aws" {
   region = "ap-southeast-2"
}

resource "aws_emr" "tf-test-cluster" {
  name          = "emr-%s"
  release_label = "emr-4.6.0"
  applications  = ["Spark"]

  master_instance_type = "m3.xlarge"
  core_instance_type   = "m3.xlarge"
  core_instance_count  = 1

  tags {
        role        = "rolename"
        dns_zone    = "env_zone"
        env         = "env"
        name        = "name-env"
  }

  bootstrap_action {
    path  ="s3://elasticmapreduce/bootstrap-actions/run-if"
    name  ="runif"
    args  =["instance.isMaster=true","echo running on master node"]
  }

  configurations = "test-fixtures/emr_configurations.json"
}
`, acctest.RandString(10))

var testAccAWSEmrConfigCore = `
provider "aws" {
   region = "ap-southeast-2"
}

resource "aws_vpc" "foo_VPC" {
	cidr_block = "10.1.0.0/16"
	enable_dns_hostnames = true
}
resource "aws_internet_gateway" "gw" {
    vpc_id = "${aws_vpc.foo_VPC.id}"
}
resource "aws_subnet" "foo_VPC" {
	cidr_block = "10.1.1.0/24"
	vpc_id = "${aws_vpc.foo_VPC.id}"
}

# Grant the VPC internet access on its main route table
resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.foo_VPC.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.gw.id}"
}

resource "aws_route_table_association" "foo_VPC" {
    subnet_id = "${aws_subnet.foo_VPC.id}"
    route_table_id = "${aws_vpc.foo_VPC.main_route_table_id}"
}

resource "aws_emr" "foo" {
  name          = "emr-core"
  release_label = "emr-4.6.0"
  applications  = ["Spark"]

  ec2_attributes {
    subnet_id      = "${aws_subnet.foo_VPC.id}"
  }

  master_instance_type = "m3.xlarge"
  core_instance_type   = "m3.xlarge"
  core_instance_count  = 1

  depends_on = ["aws_route_table_association.foo_VPC"]
}
`

var testAccAWSEmrConfigCore_resize = `
provider "aws" {
   region = "ap-southeast-2"
}

resource "aws_vpc" "foo_VPC" {
	cidr_block = "10.1.0.0/16"
	enable_dns_hostnames = true
}
resource "aws_internet_gateway" "gw" {
    vpc_id = "${aws_vpc.foo_VPC.id}"
}
resource "aws_subnet" "foo_VPC" {
	cidr_block = "10.1.1.0/24"
	vpc_id = "${aws_vpc.foo_VPC.id}"
}

# Grant the VPC internet access on its main route table
resource "aws_route" "internet_access" {
  route_table_id         = "${aws_vpc.foo_VPC.main_route_table_id}"
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = "${aws_internet_gateway.gw.id}"
}

resource "aws_route_table_association" "foo_VPC" {
    subnet_id = "${aws_subnet.foo_VPC.id}"
    route_table_id = "${aws_vpc.foo_VPC.main_route_table_id}"
}

resource "aws_emr" "foo" {
  name          = "emr-core"
  release_label = "emr-4.6.0"
  applications  = ["Spark"]

  ec2_attributes {
    subnet_id      = "${aws_subnet.foo_VPC.id}"
  }

  master_instance_type = "m3.xlarge"
  core_instance_type   = "m3.xlarge"
  core_instance_count  = 2

  depends_on = ["aws_route_table_association.foo_VPC"]
}
`
