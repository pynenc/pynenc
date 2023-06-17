"""
The BaseRunner will be a subclass of the app

  - because it needs to know about broker, orchestrator...
  - but the main reason, the runner will affect the behaviour of the task result, e.g.
    * if the running works with subprocesses may implement a pipe to communicate 
      (so it can pause/resume processes)
    * if it's an async environment, the value should be an async function to wait
      for distributed results
    * if it's a cloud function and needs to be as fast as possible with only one thread
      then it will not want to wait more than 'x' and instead, create a 'callback' save status
      convert current execution in a task that will be call when the result is ready
    * if it's a multiprocessing runner in a kubernetes pod with capabilities to create new pods
      then it may ...
    * asyncio worker, it just runs several tasks in one processor, the value should wait with async

The idea is that the user of this library can start a Runner with any configuration
(it's a subclass of Pynenc app) however, the best will be to create from the app, meaning:

Requeriments:
 - the task should run in any runner, regardless runner config
    * so if the code that calls the task and then result.value is the same
      the change of behaviour should be hidden



"""
