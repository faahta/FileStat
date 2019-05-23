#define UNICODE
#define _UNICODE

#define _CRT_SECURE_NO_WARNINGS
#define _CRT_NON_CONFORMING_SWPRINTFS

#include <windows.h>
#include <tchar.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <io.h>

TCHAR file_name[30];
DWORD N, M, count, numRecords;
CRITICAL_SECTION cs;
HANDLE hSem;      /*for writing to ouput file*/
HANDLE semPrint;  /*for printing on stdout (for the N+1th thread)*/

DWORD printFreq;



typedef struct files {
	TCHAR dirName[128];
	TCHAR inputFileNames[128];
	TCHAR outputFileName[128];

}files_t;
typedef struct stat1 {
	TCHAR fileName[30];
	DWORD numChars;
	DWORD numLines;
}stat_t;
stat_t* st;
typedef struct thread {
	DWORD tId;
	DWORD numFiles;
	stat_t* stat;
}threads_t;

DWORD WINAPI threadFunction(LPVOID);
VOID TraverseDirAndStat(threads_t*, LPTSTR, LPTSTR, LPTSTR);


int _tmain(int argc, LPTSTR argv[]) {

	/*init*/
	_tcscpy(file_name, argv[1]);
	N = _ttoi(argv[2]);
	M = _ttoi(argv[3]);
	count = 0;
	numRecords = 0;
	InitializeCriticalSection(&cs);
	hSem = CreateSemaphore(NULL, 1, 1, NULL);
	semPrint = CreateSemaphore(NULL, 1, 1, NULL);
	printFreq = 0;

	/*read file_name to get its number of records*/
	files_t fileData;
	HANDLE hFile;
	DWORD n;
	hFile = CreateFile(file_name, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	if (hFile == INVALID_HANDLE_VALUE) {
		_ftprintf(stderr, _T("error opening file\n"));
		ExitProcess(0);
	}
	while (ReadFile(hFile, &fileData, sizeof(files_t), &n, NULL) && n > 0) {
		numRecords;
	}
	_tprintf(_T("Number of Records: %d\n"), numRecords);

	/*create N+1 threads*/
	HANDLE* hThreads;
	hThreads = (HANDLE*)malloc((N + 1) * sizeof(HANDLE));
	INT i;
	threads_t* threadData;
	threadData = (threads_t*)malloc((N + 1) * sizeof(threads_t));
	for (i = 0; i <= N; i++) {
		threadData[i].tId;
		hThreads[i] = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)threadFunction, &threadData[i], 0, NULL);
		if (hThreads[i] == NULL)
			ExitProcess(0);
	}
	WaitForMultipleObjects(N + 1, hThreads, TRUE, INFINITE);
	for (i = 0; i <= N; i++) {
		CloseHandle(hThreads[i]);
	}

}

DWORD WINAPI threadFunction(LPVOID lpParam) {
	threads_t* threadData;
	threadData = (threads_t*)lpParam;

	EnterCriticalSection(&cs);
	count++;
	LeaveCriticalSection(&cs);
	if (count <= N) {
		DWORD i;
		HANDLE hFile;
		hFile = CreateFile(file_name, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
		if (hFile == INVALID_HANDLE_VALUE) {
			return 1;
		}
		while (i <= numRecords) {
			/*lock and read the file*/
			OVERLAPPED ov = { 0,0,0,0,NULL };
			LARGE_INTEGER filePos, fileReserved;
			fileReserved.QuadPart = 1 * sizeof(files_t);
			filePos.QuadPart = (i - 1) * sizeof(files_t);
			ov.Offset = filePos.LowPart;
			ov.OffsetHigh = filePos.HighPart;
			ov.hEvent = 0;
			LockFileEx(hFile, LOCKFILE_EXCLUSIVE_LOCK, 0, fileReserved.LowPart, fileReserved.HighPart, &ov);
			files_t data; DWORD n;
			ReadFile(hFile, &data, sizeof(files_t), &n, &ov);
			TraverseDirAndStat(threadData, data.dirName, data.inputFileNames, data.outputFileName);
			UnlockFileEx(hFile, 0, fileReserved.LowPart, fileReserved.HighPart, 0);
		}
	}
	else {
		WaitForSingleObject(semPrint, INFINITE);

	}

}

VOID TraverseDirAndStat(threads_t* data, LPTSTR dirName, LPTSTR inputName, LPTSTR outputName) {

	HANDLE hSearch;
	WIN32_FIND_DATA findData;
	threads_t* threadData;
	data->numFiles = 0;
	TCHAR fullPath[256]; /**/
	/*merge dirName and wildcard name(inputName)*/
	_stprintf(fullPath, _T("%s\\%s"), dirName, inputName);
	hSearch = FindFirstFile(fullPath, &findData);
	do {
		threadData->numFiles++;
		_tprintf(_T("%s\n"), findData.cFileName);
	} while (FindNextFile(hSearch, &findData));
	FindClose(hSearch);
	/*update stat*/
	hSearch = FindFirstFile(fullPath, &findData);
	threadData->stat = (stat_t*)malloc((threadData->numFiles) * sizeof(stat_t));
	do {
		INT i; /* index for each stat*/
		_tcscpy(threadData->stat[i].fileName, findData.cFileName);
		HANDLE hFile; TCHAR c; DWORD nB;
		hFile = CreateFile(findData.cFileName, GENERIC_READ, FILE_SHARE_READ, NULL, FILE_ATTRIBUTE_NORMAL, OPEN_EXISTING, NULL);
		if (hFile == INVALID_HANDLE_VALUE) {
			_ftprintf(stderr, _T("can not open file %s\n"), findData.cFileName);
			return;
		}
		while (ReadFile(hFile, &c, sizeof(TCHAR), &nB, NULL) && nB > 0) {
			if (c == '\n')
				threadData->stat[i].numLines++;
			else
				threadData->stat[i].numChars++;
		}
	} while (FindNextFile(hSearch, &findData));
	FindClose(hSearch);
	/*Write to output file*/
	WaitForSingleObject(hSem, INFINITE);
	HANDLE hOut; DWORD nW;
	hOut = CreateFile(outputName, GENERIC_WRITE, FILE_SHARE_WRITE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	if (hOut == INVALID_HANDLE_VALUE) {
		_ftprintf(stderr, _T("can not open output file %s\n"), outputName);
		return;
	}
	WriteFile(hOut, &threadData, sizeof(threads_t), &nW, NULL);
	printFreq++;
	if (printFreq == M)
		ReleaseSemaphore(semPrint, 1, NULL);
	return;
}