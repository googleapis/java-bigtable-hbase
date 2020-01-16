# This directory contains self signed certificates to test TLS

These files were generated with

```bash
openssl req -x509 -nodes -days 3650 -newkey rsa:4096 -keyout server.key -out server.crt -subj "/C=US/ST=NY/L=New York/CN=localhost/emailAddress=someEmail@example.com"

```
