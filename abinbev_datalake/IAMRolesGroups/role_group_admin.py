import boto3 as b3
import json

# Inicializa o cliente IAM
iam = b3.client('iam')

# Cria o usuário Admin
iam.create_user(UserName='superfrete-admin')

# Política completa para Admin
admin_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*"
        }
    ]
}

# Cria role para Admin e anexa a política
iam.create_role(
    RoleName='superfrete-admin-role',
    AssumeRolePolicyDocument=json.dumps(admin_policy)
)

iam.put_role_policy(
    RoleName='superfrete-admin-role',
    PolicyName='AdminAccess',
    PolicyDocument=json.dumps(admin_policy)
)

# Associa o usuário à role
iam.add_user_to_group(UserName='superfrete-admin', GroupName='superfrete-admin-role')

print("Usuário e role para Admin criados com sucesso!")
