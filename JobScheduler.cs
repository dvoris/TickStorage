using MyData.Jobs;
using Quartz;
using Quartz.Impl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyData
{
    public static class JobScheduler
    {
        public static async Task StartJobsAsync()
        {
            StdSchedulerFactory factory = new StdSchedulerFactory();

            IScheduler scheduler = await factory.GetScheduler();
            await scheduler.Start();

            IJobDetail job = JobBuilder.Create<ArchiveJob>()
                .WithIdentity("archivejob", "crongroup")
                .Build();

            ITrigger trigger = TriggerBuilder.Create()
                .WithIdentity("archivejobTrigger", "crogroup")
                .WithCronSchedule("0 50 23 * * ?")
                .Build()
                ;

        }
    }
}
