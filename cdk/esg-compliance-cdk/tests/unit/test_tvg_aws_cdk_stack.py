import aws_cdk as core
import aws_cdk.assertions as assertions

from esg_compliance_cdk.esg_compliance_cdk_stack import ESGCopmlianceCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in essg_compliance_cdk/esg_compliance_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ESGCopmlianceCdkStack(app, "esg-compliance-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
