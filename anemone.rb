#!/usr/bin/env ruby -rubygems

require 'eventmachine'
require 'em-http'
require 'evma_httpserver'
require 'libxml'

$job_counter = 0

module Anemone
  class Callback
    attr_accessor :method, :uri

    def initialize(uri, method = nil)
      @uri = uri
      @method = method || "POST"
    end
  end

  class Job
    include LibXML

    STATE_QUEUED  = "QUEUED"
    STATE_RUNNING = "RUNNING"
    STATE_DONE    = "DONE"

    attr_accessor :authorization, :callback
    attr_accessor :count, :state, :requests, :responses, :errors
    attr_reader :started_at, :completed_at
    attr_reader :id

    # <BatchRequest>
    #  <Authorization type="appid">AppName</Authorization>
    #  <CallBack method="PUT" action="http://example.com/callback" />
    #  <RequestList>
    #   <Request [key="789"] link="http://example.org/foo" />
    #   ...
    #  </RequestList>
    # </BatchRequest>
    def self.from_xml(xml)
      job = self.new
      doc = XML::Parser.string(xml).parse

      callback = doc.root.find_first("/BatchRequest/CallBack")
      job.callback = Anemone::Callback.new(callback.attributes["action"], callback.attributes["method"])

      requests = doc.root.find("/BatchRequest/RequestList/Request")
      requests.each do |r|
        job.requests << Anemone::Request.new(r.attributes["link"], r.attributes["key"])
      end

      job.count = requests.size

      job
    end

    def initialize
      @id        = $job_counter += 1
      @requests  = []
      @responses = []
      @errors    = []
      @state     = STATE_QUEUED
    end

    def complete!
      @completed_at = Time.now
      self.state = STATE_DONE

      puts "Job ##{id} took #{completed_at - started_at}s"

      # TODO make the callback
      puts "#{callback.method} #{callback.uri}"
    end

    def complete?
      if errors.size + responses.size == count
        complete!
      end
    end

    def empty?
      requests.empty?
    end

    def next_request
      start! if queued?
      requests.shift
    end

    def queued?
      state == STATE_QUEUED
    end

    def response!(request, response)
      responses << response
      complete?
    end

    def error!(request, response)
      errors << response
      complete?
    end

    def running?
      state == STATE_RUNNING
    end

    def start!
      self.state = STATE_RUNNING
      @started_at = Time.now
    end

    # <BatchStatus>
    #  <BatchJob id="12345">
    #    <state code="100">QUEUED</state>
    #    <progress total="50" complete="0" failed="0" />
    #    <link type="info" ref="http://.../batch/job/12345/status" />
    #    <link type="output" link="http://.../batch/job/12345/output" />
    #    ... TBD ...
    #  </BatchJob>
    # </BatchStatus>
    def status
      inspect
    end
  end

  class Request
    attr_accessor :key, :uri

    def initialize(uri, key = nil)
      @uri = uri
      @key = key
    end
  end
end

# TODO replace this with Sinatra middleware, spawned by Thin (since it already uses EM)
class HttpConnection  < EventMachine::Connection
  include EventMachine::HttpServer

  attr_reader :headers, :collector

  def initialize(collector)
    @collector = collector
  end

  def process_http_request
    parse_headers

    resp = EventMachine::DelegatedHttpResponse.new(self)

    case @http_request_uri
    when "/batch/job"
      case @http_request_method
      when "PUT"
        if headers["content-type"] == "application/xml"
          operation = proc do
            collector << job = Anemone::Job.from_xml(@http_post_content)
            resp.status = 201
            resp.content = job.status
          end

          callback = proc do |res|
            resp.send_response
          end

          EM.defer(operation, callback)
        else
          puts "Unhandled content-type for '#{@http_request_uri}': #{@http_request_method}"
          resp.status = 404
          resp.send_response
        end
      else
        puts "Unhandled request method for '#{@http_request_uri}': #{@http_request_method}"
        resp.status = 404
        resp.send_response
      end
    else
      # puts "Unrecognized URI: #{@http_request_uri}"
      # resp.status = 404
      # resp.send_response

      # for experimenting with long-running requests and concurrency
      operation = proc do
        # sleep rand(5000).to_f / 5000
        resp.status = 404
      end

      callback = proc do |res|
        resp.send_response
      end

      EM.defer(operation, callback)
    end
  end

protected

  def parse_headers
    @headers = {}
    raw_headers = @http_headers.split("\000")
    raw_headers.each do |h|
      key, value = h.split(":", 2)
      @headers[key.downcase] = value.strip
    end
  end
end

CONCURRENCY = 5

EM.run do
  jobs = []
  EventMachine::start_server("0.0.0.0", 8080, HttpConnection, jobs)
  puts "Listening on :8080..."

  free_connections = CONCURRENCY

  EventMachine::add_periodic_timer(0.001) do
    if free_connections == 0
      # puts "Connection pool is empty"
    end

    free_connections.times do |i|
      break if jobs.empty?

      free_connections -= 1

      job = jobs.first
      request = job.next_request

      puts "Requesting #{request.uri}..."

      http = EventMachine::HttpRequest.new(request.uri).get

      http.callback do
        case http.response_header.status.to_i
        when 200..299
          job.response!(request, http.response)
        else
          job.error!(request, http.response)
        end

        free_connections += 1
      end

      if job.empty?
        jobs.shift
      end
    end
  end

  trap(:INT) do
    puts "Shutting down..."
    EM.stop_event_loop
  end
end
