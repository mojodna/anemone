# Anemone

An HTTP server that triggers asynchronous, throttle-able HTTP requests. It
doesn't totally work (callbacks aren't called when a job completes), but it
mostly does.

a) Can be configured as a servicer of Web Hooks where the response codes are
tracked.

-or-

b) Can be configured as a batch system where responses are collected and sent
to a callback url of your choice.

## Installation

    $ sudo gem install eventmachine eventmachine_httpserver libxml
    $ sudo gem install igrigorik-em-http-request -s http://gems.github.com/

## Future Enhancements

* cleanup
* status
* Rack middleware to configure Anemone at runtime
* Pluggable job / status handlers
* PubSub support to stream results OR publish results on completion
