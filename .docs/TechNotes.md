# Tech Notes

* Trying out Mono/Flux without try
    * Why: M/F with try add much complexity in both code and reasoning, M/F already supply error handling, after using
      try for a while, I found I never used error handling provided by Reactor, and this alarmed me that I might be
      doing something wrong.
* Moving id generation to become repo responsibility instead of service
    * Why: I think adapters should be the one generating Ids, since they're the ones talking to database
    * It should be the responsibility of a service how an object is persisted, it should only care about business logic
      not implementations of how that object is saved
    * same as for controllers should ony be about converting json to DTOs and selecting which service should handle the
      request.
