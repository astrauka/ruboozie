require 'ostruct'
OpenStruct.__send__(:define_method, :id) { @table[:id] || self.object_id }

class OozieObject < OpenStruct
  attr_reader :table
end

class OozieJob < OozieObject

  def log
    OozieApi.log(job_id)
  end

  def reload
    OozieApi.info(job_id)
  end

  def definition
    OozieApi.definition(job_id)
  end

  def change(params)
    OozieApi.change_job(job_id, params)
  end

  def start
    do_action(:start)
  end

  def suspend
    do_action(:run)
  end

  def resume
    do_action(:resume)
  end

  def kill
    do_action(:kill)
  end

  def run
    do_action(:run)
  end

  def dryrun
    do_action(:dryrun)
  end

  def do_action(action)
    OozieApi.do_action(job_id, action)
  end

  def finished?
    !['RUNNING','PREP'].include?(self.status)
  end

  def failed?
    ['SUSPENDED', 'KILLED', 'FAILED'].include?(self.status)
  end

  def with_full_info
    @with_full_info ||= reload
  end

  def children
    @children ||=
      begin
        Array.wrap(
          with_full_info.bundleCoordJobs
        ).map do |job|
          OozieJob.new(job)
        end
      end
  end

  def name; appName; end
  def path; appPath; end
  def console_url; consoleUrl; end
  def created_at; Time.parse(createdTime) if createdTime; end
  def started_at; Time.parse(startTime) if startTime; end
  def ended_at; Time.parse(endTime) if endTime; end
  def external_id; externalId; end
  def actions
    @table[:actions] ||= []
    @actions = @table[:actions].map {|a| OozieAction.new(a)} unless @actions
    @actions
  end

  def job_id
    workflowJobId || coordJobId || bundleJobId
  end

  def name
    workflowJobName || coordJobName || bundleJobName
  end
end

class OozieAction < OozieObject
  def started_at; Time.parse(startTime) if startTime; end
  def ended_at; Time.parse(endTime) if endTime; end
end
