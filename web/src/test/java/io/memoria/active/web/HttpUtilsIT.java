package io.memoria.active.web;

import io.memoria.active.web.auth.BasicCredential;
import io.memoria.active.web.auth.Token;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.NoSuchElementException;

class HttpUtilsIT {

  @Test
  void basicSuccess() {
    // Given
    var header = "Basic " + Base64.getEncoder().encodeToString(("bob" + ":" + "password").getBytes());
    // When
    var t = HttpUtils.credential(header);
    // Then
    Assertions.assertEquals(new BasicCredential("bob", "password"), t.get());
  }

  @Test
  void basicExtraSpacesFail() {
    // Given
    var header = "Basic  " + Base64.getEncoder().encodeToString(("bob" + ":" + "password").getBytes());
    // When
    var t = HttpUtils.credential(header);
    // Then
    Assertions.assertInstanceOf(ArrayIndexOutOfBoundsException.class, t.getCause());
  }

  @Test
  void basicExtraSpacesSuccess() {
    // Given
    var header = "   Basic " + Base64.getEncoder().encodeToString(("bob" + ":" + "password").getBytes()) + "   ";
    // When
    var t = HttpUtils.credential(header);
    // Then
    Assertions.assertEquals(new BasicCredential("bob", "password"), t.get());
  }

  @Test
  void basicNoColonFail() {
    // Given
    var header = "   Basic " + Base64.getEncoder().encodeToString(("bob" + "" + "password").getBytes()) + "   ";
    // When
    var t = HttpUtils.credential(header);
    // Then
    Assertions.assertInstanceOf(ArrayIndexOutOfBoundsException.class, t.getCause());
  }

  @Test
  void basicNoIdFail() {
    // Given
    var header = "   Basic " + Base64.getEncoder().encodeToString(("" + ":" + "password").getBytes()) + "   ";
    // When
    var t = HttpUtils.credential(header);
    // Then
    Assertions.assertInstanceOf(IllegalArgumentException.class, t.getCause());
  }

  @Test
  void basicNoPasswordFail() {
    // Given
    var header = "   Basic " + Base64.getEncoder().encodeToString(("bob" + ":" + "").getBytes()) + "   ";
    // When
    var t = HttpUtils.credential(header);
    // Then
    Assertions.assertInstanceOf(ArrayIndexOutOfBoundsException.class, t.getCause());
  }

  @Test
  void noBasic() {
    // when
    var header = "   Base " + Base64.getEncoder().encodeToString(("bob" + "" + "password").getBytes()) + "   ";
    // When
    var t = HttpUtils.credential(header);
    // Then
    Assertions.assertInstanceOf(NoSuchElementException.class, t.getCause());
  }

  @Test
  void bearerSuccess() {
    var token = "xyz.xyz.zyz";
    var header = "Bearer " + token;
    var t = HttpUtils.credential(header);
    Assertions.assertEquals(new Token(token), t.get());
  }

  @Test
  void noBearer() {
    // Given
    var token = "xyz.xyz.zyz";
    var header = "Bearr " + token;
    // When
    var t = HttpUtils.credential(header);
    Assertions.assertInstanceOf(NoSuchElementException.class, t.getCause());
  }
}
