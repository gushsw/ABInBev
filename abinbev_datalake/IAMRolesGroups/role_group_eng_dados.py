import boto3 as b3
import json

# Inicializa o cliente IAM
iam = b3.client('iam')

# Cria o usuário de serviço Engenheiro de Dados
iam.create_user(UserName='abinbev-engenheiro-dados')

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

# Define a ´política de restrição de visualização de Billing
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

# Cria role para Engenheiro de Dados e anexa as políticas
iam.create_role(
    RoleName='abinbev-engenheiro-dados-role',
    AssumeRolePolicyDocument=json.dumps(general_data_services_policy)
)

iam.put_role_policy(
    RoleName='abinbev-engenheiro-dados-role',
    PolicyName='GeneralDataServices',
    PolicyDocument=json.dumps(general_data_services_policy)
)

iam.put_role_policy(
    RoleName='abinbev-engenheiro-dados-role',
    PolicyName='NoBillingAccess',
    PolicyDocument=json.dumps(no_billing_policy)
)

# Associa o usuário à role
iam.add_user_to_group(UserName='abinbev-engenheiro-dados', GroupName='abinbev-engenheiro-dados-role')
