
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::{cmp::Ordering, i128::MIN, ops::Deref, sync::{atomic::AtomicUsize, Arc, Mutex}};
use libc;

#[pyfunction]
fn mt_map(py_func: PyObject, data: Vec<PyObject>, num_threads: Option<usize>) -> PyResult<Vec<PyObject>> {
    let num_threads = num_threads.unwrap_or(4); // 기본적으로 4개의 스레드 사용
    let data_len = &data.len();
    let chunk_size = (data_len + num_threads - 1) / num_threads; // 데이터의 청크 크기 계산

    // 결과를 저장하기 위한 Arc<Mutex<Vec<PyObject>>> 사용
    let results = Arc::new(Mutex::new(Vec::with_capacity(*data_len)));
    let py_function = Arc::new(Mutex::new(py_func));

    // 스레드 벡터
    let mut handles = vec![];
    let chunk_indices: Vec<usize> = (0..data.len()).collect();
    let index_chunks: Vec<Vec<usize>> = chunk_indices
        .chunks(chunk_size)
        .map(|chunk| chunk.to_vec())
        .collect();
    //let data = Arc::new(data);

    for chunk in index_chunks {
            // Python GIL을 얻기 위해 py_func를 복사
            let chunk_min = chunk.iter().min().unwrap().to_owned();
            let chunk_max = chunk.iter().min().unwrap().to_owned();
            let function_in_thread = py_function.clone();
            // 결과 벡터의 Arc를 복사
            let results = Arc::clone(&results);
            let thread_data: Vec<PyAny> = chunk.iter()
            .filter_map(|&index| data.get(index))
            .collect();
            
    
            // 새로운 스레드 생성
            let handle = std::thread::spawn(move || {
                // 각 스레드는 자체 로컬 결과 벡터를 생성
                let mut local_results = vec![];
    
                // Python GIL을 사용하여 Python 함수 호출
                Python::with_gil(|py| {
                    for ch in chunk{
                    let result = function_in_thread.lock().unwrap().call1(py, (ch,)).expect("Python function call failed");
                    local_results.push(result);
                }
                });
    
                // 각 스레드가 로컬 벡터 결과를 공유된 벡터로 이동
                let mut shared_results = results.lock().unwrap();
                shared_results.extend(local_results);
            });
    
            handles.push(handle);
        }
    
    //let data_chunks: Vec<Vec<PyObject>> = data.chunks(chunk_size).map(|chunk| chunk).collect();


    // for chunk in data_chunks {
    //     // Python GIL을 얻기 위해 py_func를 복사
    //     let function_in_thread = py_function.clone();
    //     // 결과 벡터의 Arc를 복사
    //     let results = Arc::clone(&results);
        

    //     // 새로운 스레드 생성
    //     let handle = std::thread::spawn(move || {
    //         // 각 스레드는 자체 로컬 결과 벡터를 생성
    //         let mut local_results = vec![];

    //         // Python GIL을 사용하여 Python 함수 호출
    //         Python::with_gil(|py| {
    //             for ch in chunk{
    //             let result = function_in_thread.lock().unwrap().call1(py, (ch,)).expect("Python function call failed");
    //             local_results.push(result);
    //         }
    //         });

    //         // 각 스레드가 로컬 벡터 결과를 공유된 벡터로 이동
    //         let mut shared_results = results.lock().unwrap();
    //         shared_results.extend(local_results);
    //     });

    //     handles.push(handle);
    // }

    // 모든 스레드의 완료를 기다림
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // 결과를 PyResult로 반환
    let results = Arc::try_unwrap(results).expect("Failed to unwrap Arc").into_inner().unwrap();
    Ok(results)
}

#[pymodule]
fn mt_tqdm(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(mt_map, m)?)?;
    Ok(())
}

