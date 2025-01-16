from tornado import gen
import os
from jupyterhub.auth import Authenticator
import requests
import jwt
from subprocess import PIPE, STDOUT, Popen

class QCArchiveAuthenticator(Authenticator):
    
    @gen.coroutine
    def authenticate(self, handler, data):
        base_address = os.getenv("QCFRACTAL_ADDRESS")
        
        if base_address is None:
            raise RuntimeError(f"QCArchive address returned None.")
        
        login_address = f"{base_address}/auth/v1/login"
        uinfo_address = f"{base_address}/api/v1/users/{data['username']}"

        # Log in to get JWT
        body = {"username": data['username'], "password": data['password']}

        # 'verify' means whether or not to verify SSL certificates
        r = requests.post(login_address, json=body, verify=True)

        if r.status_code != 200:
            fail_msg = r.json()['msg']
            raise RuntimeError(f"Login failure: {r.status_code} - {fail_msg}")

        print("Successfully logged in!")

        # Grab access token, use to get all user information
        access_token = r.json()['access_token']

        headers = {'Authorization': f"Bearer {access_token}"}
        r = requests.get(uinfo_address, headers=headers)

        if r.status_code != 200:
            fail_msg = r.json()['msg']
            raise RuntimeError(f"Unable to get user information: {r.status_code} - {fail_msg}")

        uinfo = r.json()
        username = uinfo['username']
        user_id = uinfo['id']
        role = uinfo['role']

        cmd = ['adduser', '-q', '--disabled-password', '--uid', f'{100000+user_id}', username]
        p = Popen(cmd, stdout=PIPE, stderr=STDOUT)
        p.wait()

        os.environ['QCPORTAL_ADDRESS'] = base_address
        os.environ['QCPORTAL_USERNAME'] = username
        os.environ['QCORTAL_PASSWORD'] = data['password']
        os.environ['QCPORTAL_VERIFY'] = os.getenv("QCFRACTAL_VERIFY", "True")

        return username