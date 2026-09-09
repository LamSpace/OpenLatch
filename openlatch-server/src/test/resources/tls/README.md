# TLS 测试证书夹具（phase3-t4-security，仅测试用，勿用于生产）

一次性 openssl 预生成、随仓库提交；测试只做 PEM 文件加载，经
`SslContextBuilder` 走与生产同一条 PEM 路径（规避 JDK 强封装对 Netty
`SelfSignedCertificate` 运行时反射生成的限制）。生成命令（OpenSSL 3）：

```bash
cd tls
# 可信 CA（签 server/client 叶证书）
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 3650 \
  -subj "/CN=OpenLatch Test CA" -keyout ca-key.pem -out ca.pem

# 服务端叶证书（CA 签，含 SAN）
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server-key.pem -out server.csr
printf "subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n" > server.cnf
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -days 3650 -sha256 -extfile server.cnf -out server-cert.pem

# 客户端叶证书（CA 签，mTLS 用）
openssl req -newkey rsa:2048 -nodes -subj "/CN=openlatch-test-client" -keyout client-key.pem -out client.csr
printf "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature\nextendedKeyUsage=clientAuth\n" > client.cnf
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -days 3650 -sha256 -extfile client.cnf -out client-cert.pem

# 不可信 CA + 异 CA 签的 rogue 叶证书（双向不信任用例）
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 3650 \
  -subj "/CN=OpenLatch Other CA" -keyout other-ca-key.pem -out other-ca.pem
openssl req -newkey rsa:2048 -nodes -subj "/CN=rogue" -keyout rogue-key.pem -out rogue.csr
openssl x509 -req -in rogue.csr -CA other-ca.pem -CAkey other-ca-key.pem -CAcreateserial \
  -days 3650 -sha256 -out rogue-cert.pem

rm -f *.csr *.srl *.cnf
```

- `server-cert.pem`/`server-key.pem`：可信任（CA 签）服务端证书。
- `client-cert.pem`/`client-key.pem`：可信任（CA 签）客户端证书（mTLS）。
- `ca.pem`：可信 CA 证书（服务端/客户端 trust-store）。
- `other-ca.pem` + `rogue-cert.pem`/`rogue-key.pem`：异 CA 签的"坏"凭证，
  断言双向不信任（对端 trust-store 不含 `other-ca` 时握手失败）。
