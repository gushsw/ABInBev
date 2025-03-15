import boto3 as b3
import json

# Inicializar o cliente IAM
iam = b3.client('iam')

# Criar usuário Cientista de Dados
iam.create_user(UserName='abinbev-cientista-dados')

# Política de acesso a S3, Athena, Glue, MWAA
general_data_services_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "athena:*",
                "glue:*",
                "mwaa:*"
            ],
            "Resource": "*"
        }
    ]
}

# Política de restrição de visualização de Billing
no_billing_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Deny",
            "Action": [
                "aws-portal:ViewBilling",
                "aws-portal:ViewUsage",
                "aws-portal:ViewPaymentMethods"
            ],
            "Resource": "*"
        }
    ]
}

# Criar role para Cientista de Dados e anexar as políticas
iam.create_role(
    RoleName='abinbev-cientista-dados-role',
    AssumeRolePolicyDocument=json.dumps(general_data_services_policy)
)

iam.put_role_policy(
    RoleName='abinbev-cientista-dados-role',
    PolicyName='GeneralDataServices',
    PolicyDocument=json.dumps(general_data_services_policy)
)

iam.put_role_policy(
    RoleName='abinbev-cientista-dados-role',
    PolicyName='NoBillingAccess',
    PolicyDocument=json.dumps(no_billing_policy)
)

# Associar o usuário à role
iam.add_user_to_group(UserName='abinbev-cientista-dados', GroupName='abinbev-cientista-dados-role')
