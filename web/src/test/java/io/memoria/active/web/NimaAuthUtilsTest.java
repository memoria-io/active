package io.memoria.active.web;

class NimaAuthUtilsTest {
  // basic and token only separated for simplicity, in production it should be one path
  private static final String basicAuthPath = "/authenticate_basic";
  private static final String tokenAuthPath = "/authenticate_token";
  //  private static final DisposableServer disposableServer;
  //
  //  static {
  //    disposableServer = TestUtils.httpServerBuilder.route(routes()).bindNow();
  //  }
  //
  //  @Test
  //  @DisplayName("Should deserialize Basic authorization header correctly")
  //  void basicFromTest() {
  //    var basic = Base64.getEncoder().encodeToString(("bob:password").getBytes());
  //    Consumer<HttpHeaders> headers = b -> b.add("Authorization", "Basic " + basic);
  //    var monoResp = NettyClientUtils.post(TestUtils.httpClientBuilder.headers(headers), basicAuthPath, "payload hello");
  //    StepVerifier.create(monoResp)
  //                .expectNext(Response.of(HttpResponseStatus.OK, "(bob, password)"))
  //                .expectComplete()
  //                .verify();
  //  }
  //
  //  @Test
  //  @DisplayName("Should deserialize bearer authorization header correctly")
  //  void tokenFromTest() {
  //    var token = "xyz.xyz.xyz";
  //    Consumer<HttpHeaders> httpHeaders = b -> b.add("Authorization", "Bearer " + token);
  //    var monoResp = NettyClientUtils.get(TestUtils.httpClientBuilder.headers(httpHeaders), tokenAuthPath);
  //    StepVerifier.create(monoResp).expectNext(Response.of(HttpResponseStatus.OK, token)).expectComplete().verify();
  //  }
  //
  //  @AfterAll
  //  static void afterAll() {
  //    disposableServer.dispose();
  //  }
  //
  //  private static Consumer<HttpServerRoutes> routes() {
  //    return r -> r.get(tokenAuthPath,
  //                      (req, resp) -> NettyServerUtils.stringReply.apply(resp)
  //                                                                 .apply(HttpResponseStatus.OK,
  //                                                                        NimaAuthUtils.bearerToken(req).get()))
  //                 .post(basicAuthPath,
  //                       (req, resp) -> NettyServerUtils.stringReply.apply(resp)
  //                                                                  .apply(HttpResponseStatus.OK,
  //                                                                         NimaAuthUtils.basicCredentials(req)
  //                                                                                      .get()
  //                                                                                      .toString()));
  //  }
}
