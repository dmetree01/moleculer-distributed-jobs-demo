const crypto = require("crypto");
const Cron = require("moleculer-cron");

const jobExecutionTime = 2 * 60 * 1000;

function initJob({ 
	name = "cron", 
	cronTime = "*/2 * * * *", 
	manualStart = true, 
	timeZone = "America/Nipigon", 
	actionName = "cron.addJob",
	onTick = function() {
		this.call(actionName, { name, node: this.nodeID, })
			.then(data => this.logger.info(`cron.${name} finished`, data))
			.catch(e => this.logger.error(`cron.${name} failed`, e));
	},
	runOnInit = function() {
		this.logger.info(`${name} is created`);
	},
	onComplete = function() {
		this.logger.info(`${name} is finished`);
	},
	jobQueue = [],
}) {
	return { name, cronTime, manualStart, timeZone, onTick, runOnInit, onComplete, jobQueue, };
}

module.exports = {
	name: "cron",

	mixins: [Cron],

	crons: [
		initJob({ name: "Job1", }),
		initJob({ name: "Job2", }),
		initJob({ name: "Job3", }),
		initJob({ name: "Job4", }),
		initJob({ name: "Job5", }),
		initJob({ name: "Job6", }),
		initJob({ name: "Job7", }),
		initJob({ name: "Job8", }),
		initJob({ name: "Job9", }),
		initJob({ name: "Job10", }),
	],

	events: {
		"job.added"(ctx) {
			this.logger.info("event: job.added");

			const { name: nameFromParams, nodeID } = ctx.params;
			const job = ctx.broker.getLocalService("cron").schema.crons.find(
				({ name: storedName }) => storedName === nameFromParams 
			);
			job.jobQueue.push({
				id: crypto.randomUUID(),
				nodeID,
				createdAt: new Date,
				defer: function() {
					return new Promise((resolve) => {
						setTimeout(() => { 
							resolve(`job ${nameFromParams} finished`); 
						}, jobExecutionTime);
					});
				},
			});
		},
		"job.started"(ctx) {
			this.logger.info("event: job.started");

			const { name: nameFromParams, id, nodeID, } = ctx.params;
			const job = ctx.broker.getLocalService("cron").schema.crons.find(
				({ name: storedName }) => storedName === nameFromParams 
			);
			job.running = true;
			job.jobId = id;
			job.nodeID = nodeID;
			job.startedAt = new Date;
			job.jobQueue.shift();
		},
		"job.finished"(ctx) {
			this.logger.info("event: job.finished");

			const { name: nameFromParams } = ctx.params;
			const job = ctx.broker.getLocalService("cron").schema.crons.find(
				({ name: storedName }) => storedName === nameFromParams 
			);
			job.running = false;
			job.jobId = null;
			job.nodeID = null;
		},
	},

	actions: {
		/**
		 * Get jobs action.
		 *
		 * @returns
		 */
		jobs: {
			rest: {
				method: "GET",
				path: "/jobs"
			},
			async handler(ctx) {
				const crons = ctx.broker.getLocalService("cron").schema.crons.filter(
					(it) => it.name !== "runJobs"
				);

				return { crons };
			}
		},
		addJob: {
			handler(ctx) {
				const { name: jobNameFromParams } = ctx.params;
				const serviceSchema = ctx.broker.getLocalService("cron").schema;
				const job = serviceSchema.crons.find(
					({ name: storedJobName }) => storedJobName === jobNameFromParams 
				);
				ctx.broker.broadcast("job.added", { name: job.name, nodeID: this.broker.nodeID, });
			}
		},
	},

	created() {
		this.startJobRunner();
		
		return this.Promise.resolve();
	},

	methods: {
		startJobRunner() {
			this.schema.crons.forEach((job) => {
				if (!job.jobQueue) return;
				if (job.running) {
					this.logger.info('job already running', job.nodeID);
					return;
				}

				const enquedJob = job.jobQueue.find((it) => it.nodeID === this.broker.nodeID);

				if (enquedJob) {
					this.broker.broadcast("job.started", { 
						name: job.name, 
						id: enquedJob.id, 
						nodeID: enquedJob.nodeID, 
					});

					const { defer } = enquedJob;
					defer().then((res) => {
						this.broker.broadcast("job.finished", { name: job.name, });
					});
				} else {
					this.logger.info('jobQueue is empty for current node', job.nodeID);
				}
			});

			setTimeout(() => {
				this.startJobRunner();
			}, 1000);
		}
	},
};