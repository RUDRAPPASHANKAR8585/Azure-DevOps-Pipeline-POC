/*
=============================================
This stored procedure runs on daily basis to rollover play counts.
=============================================
Rollover Tables:
	[tuned_play]
	- [dbo].[tblTrackUser_RollingCounts]
	- [dbo].[tblRadioStationTrackUser_RollingCounts]

	[tuned_store]
	- [dbo].[tblTrack_RollingCounts]
	- [dbo].[tblRadioStationTrack_RollingCounts]


Test:
EXEC [dbo].[spMaintHistoryUpdate]

Change Logs:
	26 Aug 2015 MK - Update from populating PlayStats tables to RollingCounts tables
	06 Sep 2015 MK - update to include rolling tables from tuned_store
	11 Sep 2015 MK - remove all rolling tables from tuned_store (ambiguous)
	07 Jan 2016 RZ - rewrite, for modified tables and using differnt logic + adding in store totals which are important
*/
CREATE PROCEDURE [dbo].[spMaintHistoryUpdate]
AS
	SET NOCOUNT ON;

	DECLARE @loc_curr_date AS DATE, @loc_today_date AS DATE, @loc_week_date AS DATE, @loc_month_date AS DATE, @loc_date_type AS NCHAR(1)

	SET @loc_date_type = 'D';
	SET @loc_curr_date = CAST(GETUTCDATE() AS DATE);


	/*********************************
		1.1 rollover tblRadioStationTrackUser_RollingCounts
	*********************************/

	BEGIN TRY

		UPDATE [tblRadioStationTrackUser_RollingCounts]
		SET [current_date]=@loc_curr_date
      ,[current_day_plays] = 0
      ,[current_day_skips] = 0
      ,[current_day_likes] = 0
      ,[current_day_dislikes] = 0
      ,[current_day_shares] = 0
      ,[current_week_plays] = a.[current_week_plays]
      ,[current_week_skips] = a.[current_week_skips]
      ,[current_week_likes] = a.[current_week_likes]
      ,[current_week_dislikes] = a.[current_week_dislikes]
      ,[current_week_shares] = a.[current_week_shares]
      ,[current_month_plays] = a.[current_month_plays]
      ,[current_month_skips] = a.[current_month_skips]
      ,[current_month_likes] = a.[current_month_likes]
      ,[current_month_dislikes] = a.[current_month_dislikes]
      ,[current_month_shares] = a.[current_month_shares]
		FROM [tblRadioStationTrackUser_RollingCounts] c WITH (NOLOCK)
		INNER JOIN
			-- do the current date filtering inside to sub to avoid unnecessary aggregating
			(SELECT c.[group_id], c.[station_id], c.[track_id], c.[user_id]
			  ,[current_week_plays] = sum(case when uc.[startdate] > dateadd(dd, -7, @loc_curr_date) then uc.[streams_count] else 0 end)
			  ,[current_week_skips] =  sum(case when uc.[startdate] > dateadd(dd, -7, @loc_curr_date) then uc.[skips_count] else 0 end)
			  ,[current_week_likes] =  sum(case when uc.[startdate] > dateadd(dd, -7, @loc_curr_date) then uc.[likes_count] else 0 end)
			  ,[current_week_dislikes] =  sum(case when uc.[startdate] > dateadd(dd, -7, @loc_curr_date) then uc.[dislikes_count] else 0 end)
			  ,[current_week_shares] =  sum(case when uc.[startdate] > dateadd(dd, -7, @loc_curr_date) then uc.[shares_count] else 0 end)
			  ,[current_month_plays] = sum(case when uc.[startdate] > dateadd(dd, -30, @loc_curr_date) then uc.[streams_count] else 0 end)
			  ,[current_month_skips] =  sum(case when uc.[startdate] > dateadd(dd, -30, @loc_curr_date) then uc.[skips_count] else 0 end)
			  ,[current_month_likes] =  sum(case when uc.[startdate] > dateadd(dd, -30, @loc_curr_date) then uc.[likes_count] else 0 end)
			  ,[current_month_dislikes] =  sum(case when uc.[startdate] > dateadd(dd, -30, @loc_curr_date) then uc.[dislikes_count] else 0 end)
			  ,[current_month_shares] =  sum(case when uc.[startdate] > dateadd(dd, -30, @loc_curr_date) then uc.[shares_count] else 0 end)
			FROM [tblRadioStationTrackUser_RollingCounts] c WITH (NOLOCK)
			JOIN [tblRadioStationTrackUser_Counts] uc WITH (NOLOCK) on c.[group_id] = uc.[group_id] and c.[station_id] = uc.[station_id] and c.[track_id] = uc.[track_id] and c.[user_id] = uc.[user_id]
			WHERE c.[current_date] <> @loc_curr_date
			AND uc.[date_type] = 'D'
			AND uc.[startdate] > dateadd(dd, -40, @loc_curr_date)
			GROUP BY c.[group_id], c.[station_id], c.[track_id], c.[user_id]) as a  on c.[group_id] = a.[group_id] and c.[station_id] = a.[station_id] and c.[track_id] = a.[track_id] and c.[user_id] = a.[user_id]

	END TRY
	BEGIN CATCH
		-- ** should report error if exists
		PRINT '-- ERROR rollover tblRadioStationTrackUser_RollingCounts --';
	END CATCH


	/*********************************
		1.2 rollover tblRadioStationTrack_RollingCounts (using updated tblRadioStationTrackUser_RollingCounts)
	*********************************/

	BEGIN TRY

		UPDATE [tuned_store].[dbo].[tblRadioStationTrack_RollingCounts]
		SET [current_date]=@loc_curr_date
			,[current_day_plays] = 0
			,[current_day_skips] = 0
			,[current_day_likes] = 0
			,[current_day_dislikes] = 0
			,[current_day_shares] = 0
			,[current_day_users] = 0
			,[current_day_skipusers] = 0
			,[current_day_likeusers] = 0
			,[current_day_dislikeusers] = 0
			,[current_day_shareusers] = 0
			,[current_week_plays] = a.[current_week_plays]
			,[current_week_skips] = a.[current_week_skips]
			,[current_week_likes] = a.[current_week_likes]
			,[current_week_dislikes] = a.[current_week_dislikes]
			,[current_week_shares] = a.[current_week_shares]
			,[current_week_users] = a.[current_week_users]
			,[current_week_skipusers] = a.[current_week_skipusers]
			,[current_week_likeusers] = a.[current_week_likeusers]
			,[current_week_dislikeusers] = a.[current_week_dislikeusers]
			,[current_week_shareusers] = a.[current_week_shareusers]
			,[current_month_plays] = a.[current_month_plays]
			,[current_month_skips] = a.[current_month_skips]
			,[current_month_likes] = a.[current_month_likes]
			,[current_month_dislikes] = a.[current_month_dislikes]
			,[current_month_shares] = a.[current_month_shares]
			,[current_month_users] = a.[current_month_users]
			,[current_month_skipusers] = a.[current_month_skipusers]
			,[current_month_likeusers] = a.[current_month_likeusers]
			,[current_month_dislikeusers] = a.[current_month_dislikeusers]
			,[current_month_shareusers] = a.[current_month_shareusers]
		FROM [tuned_store].[dbo].[tblRadioStationTrack_RollingCounts] c WITH (NOLOCK)
		INNER JOIN
			-- do the current date filtering inside to sub to avoid unnecessary aggregating
			(SELECT c.[group_id], c.[station_id], c.[track_id]
			  ,[current_week_plays] = sum(uc.[current_week_plays])
			  ,[current_week_skips] =  sum(uc.[current_week_skips])
			  ,[current_week_likes] =  sum(uc.[current_week_likes])
			  ,[current_week_dislikes] =  sum(uc.[current_week_dislikes])
			  ,[current_week_shares] =  sum(uc.[current_week_shares])
			  ,[current_week_users] =  sum(case when uc.[current_week_plays] > 0 then 1 else 0 end)
			  ,[current_week_skipusers] = sum(case when uc.[current_week_skips] > 0 then 1 else 0 end)
			  ,[current_week_likeusers] = sum(case when uc.[current_week_likes] > 0 then 1 else 0 end)
			  ,[current_week_dislikeusers] = sum(case when uc.[current_week_dislikes] > 0 then 1 else 0 end)
			  ,[current_week_shareusers] = sum(case when uc.[current_week_shares] > 0 then 1 else 0 end)
			  ,[current_month_plays] = sum(uc.[current_month_plays])
			  ,[current_month_skips] =  sum(uc.[current_month_skips])
			  ,[current_month_likes] =  sum(uc.[current_month_likes])
			  ,[current_month_dislikes] =  sum(uc.[current_month_dislikes])
			  ,[current_month_shares] =  sum(uc.[current_month_shares])
			  ,[current_month_users] =  sum(case when uc.[current_month_plays] > 0 then 1 else 0 end)
			  ,[current_month_skipusers] = sum(case when uc.[current_month_skips] > 0 then 1 else 0 end)
			  ,[current_month_likeusers] = sum(case when uc.[current_month_likes] > 0 then 1 else 0 end)
			  ,[current_month_dislikeusers] = sum(case when uc.[current_month_dislikes] > 0 then 1 else 0 end)
			  ,[current_month_shareusers] = sum(case when uc.[current_month_shares] > 0 then 1 else 0 end)
			FROM [tuned_store].[dbo].[tblRadioStationTrack_RollingCounts] c WITH (NOLOCK)
			JOIN [tblRadioStationTrackUser_RollingCounts] uc WITH (NOLOCK) on c.[group_id] = uc.[group_id] and c.[station_id] = uc.[station_id] and c.[track_id] = uc.[track_id]
			WHERE c.[current_date] <> @loc_curr_date
			GROUP BY c.[group_id], c.[station_id], c.[track_id]) as a  on c.[group_id] = a.[group_id] and c.[station_id] = a.[station_id] and c.[track_id] = a.[track_id]

	END TRY
	BEGIN CATCH
		-- ** should report error if exists
		PRINT '-- ERROR rollover tblRadioStationTrack_RollingCounts --';
	END CATCH



	/*********************************
		2.1 update wilson score lower bound confidence intervals on tblRadioStationTrack_RollingCounts
	*********************************/

	BEGIN TRY
		UPDATE [tuned_store].dbo.tblRadioStationTrack_RollingCounts
		SET wilson_score = t.wilson_score
		FROM [tuned_store].[dbo].tblRadioStationTrack_RollingCounts c
		INNER JOIN (
			SELECT rtrc.group_id, rtrc.station_id, rtrc.track_id, dbo.fnCalculateWilsonScore(
				(trc.current_month_users * 0.1) + (rtrc.current_day_users * 0.8) + rtrc.current_week_likeusers + (rtrc.current_month_users * 0.5) + rtrc.total_likes + 0.001,
				(trc.current_month_skipusers * 0.2) + rtrc.current_day_skipusers + rtrc.current_week_dislikeusers + rtrc.current_month_skipusers + rtrc.total_dislikes + 0.001
			   ) AS wilson_score
			FROM [tuned_store].[dbo].tblRadioStationTrack_RollingCounts rtrc
			INNER JOIN [tuned_store].[dbo].tblTrack_RollingCounts trc ON trc.[group_id]=rtrc.[group_id] AND trc.[track_id]=rtrc.[track_id]
		)  as t ON c.[group_id]=t.[group_id] AND c.[station_id]=t.[station_id] AND c.[track_id]=t.[track_id]

	END TRY
	BEGIN CATCH
		-- ** something went wrong!
		PRINT '-- ERROR updating wilson score on tblRadioStationTrack_RollingCounts --';
	END CATCH


	/*********************************
		2.2 update wilson score lower bound confidence intervals on tblRadioStationTrackUser_RollingCounts
	*********************************/

	BEGIN TRY
		UPDATE tblRadioStationTrackUser_RollingCounts
		SET wilson_score = t.wilson_score
		FROM tblRadioStationTrackUser_RollingCounts c
		INNER JOIN (
			SELECT rtrc.group_id, rtrc.user_id, rtrc.station_id, rtrc.track_id, dbo.fnCalculateWilsonScore(
					(trc.current_month_plays * 0.1) + (rtrc.current_day_plays * 0.8) + rtrc.current_week_likes + (rtrc.current_month_plays * 0.5) + rtrc.total_likes + 0.001,
					(trc.current_month_skips * 0.2) + rtrc.current_day_skips + rtrc.current_week_dislikes + rtrc.current_month_skips + rtrc.total_dislikes + 0.001
				   ) AS wilson_score
			FROM tblRadioStationTrackUser_RollingCounts rtrc
			INNER JOIN tblTrackUser_RollingCounts trc ON trc.[group_id]=rtrc.[group_id] AND trc.[track_id]=rtrc.[track_id] AND trc.[user_id]=rtrc.[user_id]
		)  as t ON c.[group_id]=t.[group_id] AND c.[station_id]=t.[station_id] AND c.[track_id]=t.[track_id] AND c.[user_id]=t.[user_id]

	END TRY
	BEGIN CATCH
		-- ** something went wrong!
		PRINT '-- ERROR updating wilson score on tblRadioStationTrackUser_RollingCounts --';
	END CATCH
